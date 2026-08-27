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

#include "exec/pipeline/sink/greenplum_table_sink_operator.h"

#include <fmt/format.h>

#include "util/time.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "connector/greenplum/greenplum_codec.h"
#include "connector/greenplum/gpfdist_session.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/expr.h"
#include "storage/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "udf/java/utils.h"

namespace starrocks::pipeline {

// Writes chunks to a Greenplum/ADB external table by encoding them into GP
// TEXT-format blocks (GreenplumTextEncoder) and handing those blocks to the
// gpfdist PullSession that the Greenplum segments are pulling from
// (SessionRegistry, keyed by the session token the FE handed out).
//
// GreenplumTextEncoder::encode() looks columns up by slot id
// (Chunk::get_column_by_slot_id / get_slot_id_to_index_map), so it needs a
// real TupleDescriptor - not just the output exprs' bare types. But
// TGreenplumTableSink carries only column names and the output exprs (no
// FE-registered tuple id for this sink's output schema, unlike e.g. OLAP
// table sink's OlapTableSchemaParam). So this class builds a synthetic
// TupleDescriptor itself, one slot per output expr in target-column order,
// via DescriptorTbl::create() + the TDescriptorTableBuilder helpers - the
// same idiom IcebergDeleteSink uses to build an ad hoc tuple descriptor for
// its (file_path, pos) delete-file schema (see
// connector/iceberg_delete_sink.cpp).
class GreenplumTableSinkIOBuffer final : public SinkIOBuffer {
public:
    GreenplumTableSinkIOBuffer(const TGreenplumTableSink& t_greenplum_table_sink,
                               std::vector<ExprContext*>& output_expr_ctxs, int32_t num_sinkers,
                               FragmentContext* fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _t_greenplum_table_sink(t_greenplum_table_sink),
              _output_expr_ctxs(output_expr_ctxs),
              _fragment_ctx(fragment_ctx) {}

    ~GreenplumTableSinkIOBuffer() override = default;

    // Open the gpfdist pull session EAGERLY at prepare time (exactly once via
    // _open_once), so a fragment instance that receives zero chunks still
    // registers its session. Otherwise a Greenplum segment pulling that
    // location slot would get a 404 and abort the whole INSERT.
    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _add_chunk(const ChunkPtr& chunk) override;

    // Builds the synthetic tuple descriptor + encoder and opens the gpfdist
    // pull session. Runs exactly once (guarded by _open_once in prepare()).
    Status _open_greenplum_sink(RuntimeState* state);

    TGreenplumTableSink _t_greenplum_table_sink;
    const std::vector<ExprContext*> _output_expr_ctxs;

    std::atomic<bool> _open_once{false};
    TupleDescriptor* _tuple_desc = nullptr;
    std::unique_ptr<connector::GreenplumTextEncoder> _encoder;
    std::shared_ptr<connector::gpfdist::PullSession> _pull_session;
    FragmentContext* _fragment_ctx;
};

Status GreenplumTableSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    RETURN_IF_ERROR(SinkIOBuffer::prepare(state, parent_profile));
    // First caller to win the CAS opens the session. Output exprs are already
    // prepared by the factory, and all prepare() calls complete before any
    // chunk is pushed, so this is race-free w.r.t. _add_chunk.
    bool expected = false;
    if (_open_once.compare_exchange_strong(expected, true)) {
        RETURN_IF_ERROR(_open_greenplum_sink(state));
    }
    return Status::OK();
}

Status GreenplumTableSinkIOBuffer::_open_greenplum_sink(RuntimeState* state) {
    DCHECK(_pull_session == nullptr);

    // Build one slot per output expr, in target-column order, typed from the
    // expr's own (already target-schema-cast) type.
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;
    const size_t num_cols = _output_expr_ctxs.size();
    for (size_t i = 0; i < num_cols; ++i) {
        TSlotDescriptorBuilder slot_builder;
        std::string col_name = i < _t_greenplum_table_sink.column_names.size()
                                        ? _t_greenplum_table_sink.column_names[i]
                                        : fmt::format("col_{}", i);
        tuple_builder.add_slot(slot_builder.id(static_cast<int>(i))
                                       .type(_output_expr_ctxs[i]->root()->type())
                                       .nullable(true)
                                       .is_materialized(true)
                                       .column_name(col_name)
                                       .build());
    }
    tuple_builder.build(&desc_tbl_builder);
    TDescriptorTable t_desc_tbl = desc_tbl_builder.desc_tbl();

    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(state, state->obj_pool(), t_desc_tbl, &desc_tbl, state->chunk_size()));
    _tuple_desc = desc_tbl->get_tuple_descriptor(0);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("failed to build greenplum table sink tuple descriptor");
    }

    _encoder = std::make_unique<connector::GreenplumTextEncoder>(
            _tuple_desc, _t_greenplum_table_sink.column_separator, _t_greenplum_table_sink.null_marker);

    _pull_session = connector::gpfdist::SessionRegistry::instance()->create_pull(
            _t_greenplum_table_sink.session_token, config::greenplum_gpfdist_session_buffer_bytes);
    if (_pull_session == nullptr) {
        return Status::InternalError("failed to create greenplum gpfdist pull session");
    }
    return Status::OK();
}

void GreenplumTableSinkIOBuffer::close(RuntimeState* state) {
    if (_pull_session != nullptr) {
        if (is_cancelled() || !get_io_status().ok()) {
            Status fail_status = get_io_status();
            if (fail_status.ok()) {
                fail_status = Status::Cancelled("greenplum table sink cancelled");
            }
            _pull_session->fail(fail_status);
        } else {
            _pull_session->finish();
            TGreenplumSinkInfo sink_info;
            sink_info.__set_rows_written(_pull_session->rows_served());
            sink_info.__set_location_slot(_t_greenplum_table_sink.location_slot);
            TSinkCommitInfo commit_info;
            commit_info.__set_greenplum_sink_info(sink_info);
            state->add_sink_commit_info(commit_info);
        }
        connector::gpfdist::SessionRegistry::instance()->remove(_t_greenplum_table_sink.session_token,
                                                                 MonotonicMillis());
        _pull_session.reset();
    }
    _encoder.reset();
    SinkIOBuffer::close(state);
}

void GreenplumTableSinkIOBuffer::_add_chunk(const ChunkPtr& chunk) {
    if (_pull_session == nullptr) {
        // prepare() opens the session eagerly; a null here means prepare failed.
        _fragment_ctx->cancel(Status::InternalError("greenplum sink session not opened before add_chunk"));
        return;
    }

    if (chunk == nullptr || chunk->is_empty()) {
        return;
    }

    const size_t num_rows = chunk->num_rows();
    const size_t num_cols = _output_expr_ctxs.size();

    // Evaluate the output exprs into a chunk keyed by the synthetic tuple
    // descriptor's slot ids (one slot per output expr, in order), since
    // GreenplumTextEncoder::encode() looks columns up by slot id. This
    // mirrors MysqlTableWriter::append (which evaluates the same
    // _output_expr_ctxs into a plain Columns array) but additionally has to
    // unfold const columns, because unlike ColumnViewer (used by the mysql
    // path) the encoder indexes rows directly and expects every column to
    // physically have `num_rows` rows.
    ChunkUniquePtr out_chunk = ChunkHelper::new_chunk(*_tuple_desc, num_rows);
    for (size_t i = 0; i < num_cols; ++i) {
        auto result = _output_expr_ctxs[i]->evaluate(chunk.get());
        if (!result.ok()) {
            LOG(WARNING) << "evaluate greenplum sink output expr failed, error: " << result.status().to_string();
            _pull_session->fail(result.status());
            _fragment_ctx->cancel(result.status());
            return;
        }
        ColumnPtr col =
                ColumnHelper::unfold_const_column(_output_expr_ctxs[i]->root()->type(), num_rows, result.value());
        out_chunk->update_column(std::move(col), _tuple_desc->slots()[i]->id());
    }

    std::string block;
    if (Status status = _encoder->encode(*out_chunk, &block); !status.ok()) {
        LOG(WARNING) << "encode greenplum sink chunk failed, error: " << status.to_string();
        _pull_session->fail(status);
        _fragment_ctx->cancel(status);
        return;
    }

    if (Status status = _pull_session->put_block(std::move(block), config::greenplum_gpfdist_session_ttl_ms);
        !status.ok()) {
        LOG(WARNING) << "put block to greenplum gpfdist pull session failed, error: " << status.to_string();
        _pull_session->fail(status);
        _fragment_ctx->cancel(status);
        return;
    }
    _pull_session->add_rows_served(static_cast<int64_t>(num_rows));
}

Status GreenplumTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _greenplum_table_sink_buffer->prepare(state, _unique_metrics.get());
}

void GreenplumTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool GreenplumTableSinkOperator::need_input() const {
    return _greenplum_table_sink_buffer->need_input();
}

bool GreenplumTableSinkOperator::is_finished() const {
    return _greenplum_table_sink_buffer->is_finished();
}

Status GreenplumTableSinkOperator::set_finishing(RuntimeState* state) {
    return _greenplum_table_sink_buffer->set_finishing();
}

bool GreenplumTableSinkOperator::pending_finish() const {
    return !_greenplum_table_sink_buffer->is_finished();
}

Status GreenplumTableSinkOperator::set_cancelled(RuntimeState* state) {
    _greenplum_table_sink_buffer->cancel_one_sinker();
    return Status::OK();
}

StatusOr<ChunkPtr> GreenplumTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from greenplum table sink operator");
}

Status GreenplumTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _greenplum_table_sink_buffer->append_chunk(state, chunk);
}

GreenplumTableSinkOperatorFactory::GreenplumTableSinkOperatorFactory(int32_t id,
                                                                     const TGreenplumTableSink& t_greenplum_table_sink,
                                                                     std::vector<TExpr> t_output_expr,
                                                                     int32_t num_sinkers, FragmentContext* fragment_ctx)
        : OperatorFactory(id, "greenplum_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _t_greenplum_table_sink(t_greenplum_table_sink),
          _num_sinkers(num_sinkers),
          _fragment_ctx(fragment_ctx) {}

Status GreenplumTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    _greenplum_table_sink_buffer = std::make_shared<GreenplumTableSinkIOBuffer>(_t_greenplum_table_sink,
                                                                                _output_expr_ctxs, _num_sinkers,
                                                                                _fragment_ctx);

    return Status::OK();
}

void GreenplumTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
