// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "connector/greenplum/greenplum_connector.h"

#include "common/config.h"
#include "util/time.h"
#include "column/chunk.h"
#include "connector/greenplum/greenplum_codec.h"
#include "connector/greenplum/gpfdist_server.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

DataSourceProviderPtr GreenplumConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                      const TPlanNode& plan_node) const {
    return std::make_unique<GreenplumDataSourceProvider>(scan_node, plan_node);
}

GreenplumDataSourceProvider::GreenplumDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _greenplum_scan_node(plan_node.greenplum_scan_node) {}

DataSourcePtr GreenplumDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<GreenplumDataSource>(this, scan_range);
}

const TupleDescriptor* GreenplumDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_greenplum_scan_node.tuple_id);
}

GreenplumDataSource::GreenplumDataSource(const GreenplumDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

std::string GreenplumDataSource::name() const {
    return "GreenplumDataSource";
}

Status GreenplumDataSource::open(RuntimeState* state) {
    const TGreenplumScanNode& tnode = _provider->_greenplum_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(tnode.tuple_id);

    if (!tnode.__isset.session_token || tnode.session_token.empty()) {
        return Status::NotSupported(
                "greenplum scan requires the gpfdist transport: the FE read orchestrator did not set a "
                "session token (only the write path is wired for this catalog so far)");
    }

    // Make sure the local gpfdist listener is up: GP segments will connect to
    // it and push this scan's rows. The FE created a WRITABLE external table on
    // Greenplum whose LOCATION points here under our session token, and it runs
    // the driving INSERT concurrently (see GreenplumReadOrchestrator on the FE).
    RETURN_IF_ERROR(gpfdist::GpfdistServer::start_once(config::greenplum_gpfdist_port));

    // Register BEFORE the segments connect (the FE opens this fragment before
    // issuing the GP statement, so no race): a PushSession they append to.
    _session = gpfdist::SessionRegistry::instance()->create_push(
            tnode.session_token, config::greenplum_gpfdist_session_buffer_bytes);
    gpfdist::SessionRegistry::instance()->touch(tnode.session_token, MonotonicMillis());

    std::string sep = tnode.__isset.column_separator ? tnode.column_separator : std::string("\t");
    std::string null_marker = tnode.__isset.null_marker ? tnode.null_marker : std::string("\\N");
    _decoder = std::make_unique<GreenplumTextDecoder>(_tuple_desc, sep, null_marker);
    return Status::OK();
}

void GreenplumDataSource::close(RuntimeState* state) {
    const TGreenplumScanNode& tnode = _provider->_greenplum_scan_node;
    if (_session != nullptr) {
        // On a clean EOF the session is already drained; failing it is a no-op
        // (the failure is only recorded if none is set yet). On an abnormal
        // close (query cancelled) this unblocks any segment still POSTing.
        _session->fail(Status::Aborted("greenplum scan closed"));
        _session.reset();
    }
    if (tnode.__isset.session_token && !tnode.session_token.empty()) {
        gpfdist::SessionRegistry::instance()->remove(tnode.session_token, MonotonicMillis());
    }
}

Status GreenplumDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(_init_chunk_if_needed(chunk, state->chunk_size()));
    const int64_t take_timeout_ms = config::greenplum_gpfdist_session_ttl_ms;

    while ((*chunk)->num_rows() == 0) {
        // Decode whatever we already carried over first; a row may straddle
        // two POST bodies, so `_pending` holds the trailing partial row.
        if (!_pending.empty()) {
            int64_t rows = 0;
            ASSIGN_OR_RETURN(size_t consumed, _decoder->decode(_pending, chunk->get(), &rows));
            _pending.erase(0, consumed);
            _rows_read += rows;
            if ((*chunk)->num_rows() > 0) break;
        }
        // Block for the next slab of pushed bytes.
        ASSIGN_OR_RETURN(std::optional<std::string> slab, _session->take(take_timeout_ms));
        if (!slab.has_value()) {
            // All segments sent DONE and the buffer drained. Any bytes still in
            // `_pending` here would be a truncated final row = corrupt input.
            if (!_pending.empty()) {
                return Status::Corruption("greenplum scan ended with an incomplete final row");
            }
            if ((*chunk)->num_rows() > 0) return Status::OK();
            return Status::EndOfFile("greenplum scan complete");
        }
        _bytes_read += static_cast<int64_t>(slab->size());
        _pending.append(*slab);
    }
    return Status::OK();
}

} // namespace starrocks::connector
