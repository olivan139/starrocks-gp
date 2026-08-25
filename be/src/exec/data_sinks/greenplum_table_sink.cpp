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

#include "exec/data_sinks/greenplum_table_sink.h"

#include <sstream>

#include "common/runtime_profile.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/runtime_state.h"

namespace starrocks {

GreenplumTableSink::GreenplumTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                       const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

GreenplumTableSink::~GreenplumTableSink() = default;

Status GreenplumTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    _t_greenplum_table_sink = t_sink.greenplum_table_sink;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs, state));
    return Status::OK();
}

Status GreenplumTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(ExprExecutor::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "GreenplumTableSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status GreenplumTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(ExprExecutor::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status GreenplumTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    // Unused: the pipeline execution path (GreenplumTableSinkOperator /
    // GreenplumTableSinkIOBuffer) drives writes through a gpfdist PullSession
    // instead of going through this non-pipeline DataSink API.
    return Status::NotSupported("GreenplumTableSink only supports the pipeline execution engine");
}

Status GreenplumTableSink::close(RuntimeState* state, const Status& exec_status) {
    ExprExecutor::close(_output_expr_ctxs, state);
    return Status::OK();
}

} // namespace starrocks
