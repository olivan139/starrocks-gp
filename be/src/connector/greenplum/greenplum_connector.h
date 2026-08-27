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

#pragma once

#include <memory>

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "connector/greenplum/gpfdist_session.h"
#include "connector/greenplum/greenplum_codec.h"

// Greenplum (Arenadata DB) connector, BE side. gpfdist-only design:
//  - this BE NEVER connects to Greenplum (no credentials here!). The FE holds
//    the control connection; GP *segments* connect to us over the gpfdist
//    protocol served by GpfdistServer (Stage 2).
//  - READ path ("SELECT ... FROM greenplum...", "INSERT INTO sr SELECT..."):
//    the FE creates a WRITABLE external table on GP whose INSERT pushes rows
//    to our gpfdist endpoint; GreenplumDataSource drains the PushSession and
//    decodes rows into chunks.
// Contract: GREENPLUM_BE_CONTRACT.md. Wire spec: ADB_GPFDIST_PROTOCOL_NOTES.md.

namespace starrocks {
class RuntimeState;
class TupleDescriptor;
} // namespace starrocks

namespace starrocks::connector {

class GreenplumConnector final : public Connector {
public:
    ~GreenplumConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    // Stage 3: return the GreenplumChunkSinkProvider here (write path).
    // std::unique_ptr<ConnectorChunkSinkProvider> create_data_sink_provider() const override;

    ConnectorType connector_type() const override { return ConnectorType::GREENPLUM; }
};

class GreenplumDataSourceProvider;

class GreenplumDataSourceProvider final : public DataSourceProvider {
public:
    friend class GreenplumDataSource;
    ~GreenplumDataSourceProvider() override = default;
    GreenplumDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);

    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    // Single-node scan (FE plans it UNPARTITIONED, like JDBC): one instance,
    // local exchange above it, no empty-range morsels.
    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TGreenplumScanNode _greenplum_scan_node;
};

class GreenplumDataSource final : public DataSource {
public:
    ~GreenplumDataSource() override = default;

    GreenplumDataSource(const GreenplumDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override { return _rows_read; }
    int64_t num_rows_read() const override { return _rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_ns; }

private:
    const GreenplumDataSourceProvider* _provider;
    RuntimeState* _runtime_state = nullptr;

    // The gpfdist POST half fills this; get_next() drains it.
    std::shared_ptr<gpfdist::PushSession> _session;
    std::unique_ptr<GreenplumTextDecoder> _decoder;
    // Carry-over bytes of a row split across two POST bodies.
    std::string _pending;

    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

} // namespace starrocks::connector
