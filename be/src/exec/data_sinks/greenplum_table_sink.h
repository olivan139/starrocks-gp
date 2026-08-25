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

#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks {

class RowDescriptor;
class TExpr;
class RuntimeState;
class RuntimeProfile;
class ExprContext;

// This class is a sinker for INSERT INTO a Greenplum/ADB external table.
// Unlike MysqlTableSink it never performs IO on this object directly:
// send_chunk() is unused because the pipeline execution path
// (GreenplumTableSinkOperator/GreenplumTableSinkIOBuffer, see
// exec/pipeline/sink/greenplum_table_sink_operator.h) drives the writes
// asynchronously through a gpfdist PullSession. This mirrors MysqlTableSink,
// whose actual IO also only happens through the pipeline operator.
class GreenplumTableSink final : public DataSink {
public:
    GreenplumTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs);

    ~GreenplumTableSink() override;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    Status close(RuntimeState* state, const Status& exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

    std::vector<TExpr> get_output_expr() const { return _t_output_expr; }

    const TGreenplumTableSink& get_t_greenplum_table_sink() const { return _t_greenplum_table_sink; }

private:
    [[maybe_unused]] ObjectPool* _pool;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<ExprContext*> _output_expr_ctxs;

    TGreenplumTableSink _t_greenplum_table_sink;

    RuntimeProfile* _profile = nullptr;
};

} // namespace starrocks
