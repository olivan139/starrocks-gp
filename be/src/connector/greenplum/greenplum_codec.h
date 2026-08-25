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
#include <string>
#include <string_view>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"

// Greenplum TEXT-format row codec (both directions).
//
// Format contract (must equal what the FE puts in the external table DDL,
// carried per-query in TGreenplumScanNode/TGreenplumTableSink):
//   - column separator: default TAB;  NULL marker: default \N
//   - escapes inside values: backslash before separator, newline, backslash
//   - one row per newline; rows never straddle a served BLOCK (write path),
//     but MAY straddle POST bodies (read path - caller keeps the tail)
//
// Decode: prefer wrapping be/src/formats/csv/ (vectorized converters per
// LogicalType) over hand-parsing - see csv_reader usage in file_connector.
//
// CORRECTNESS RULE (non-negotiable, see contract doc): a value that cannot be
// parsed into the slot type MUST fail the query - never silently null it out.
//
// TODO(stage1): implement. The type edge cases that will bite, with tests to
// write FIRST: numeric->DECIMAL128 overflow policy, DATE (no epoch tricks -
// text roundtrip), DATETIME (values are UTC by contract), backslash escapes,
// empty string vs \N.

namespace starrocks {
class TupleDescriptor;
namespace csv {
class Converter;
} // namespace csv
} // namespace starrocks

namespace starrocks::connector {

class GreenplumTextDecoder {
public:
    GreenplumTextDecoder(const TupleDescriptor* tuple_desc, std::string column_separator, std::string null_marker);
    ~GreenplumTextDecoder();

    // Decode all COMPLETE rows in `data` into `chunk` (append). Returns the
    // number of bytes consumed; the caller re-offers unconsumed tail bytes
    // together with the next slab. `rows_out` += rows appended.
    // Any unparseable value FAILS (Corruption) - never silently nulled.
    StatusOr<size_t> decode(std::string_view data, Chunk* chunk, int64_t* rows_out);

private:
    Status _decode_row(std::string_view row, Chunk* chunk);

    const TupleDescriptor* _tuple_desc;
    const std::string _column_separator;
    const std::string _null_marker;
    std::vector<std::unique_ptr<csv::Converter>> _converters; // per slot, non-nullable base
    std::string _field_buf;                                   // unescape scratch
};

class GreenplumTextEncoder {
public:
    GreenplumTextEncoder(const TupleDescriptor* tuple_desc, std::string column_separator, std::string null_marker);
    ~GreenplumTextEncoder();

    // Append all rows of `chunk` to `out` as TEXT rows (with escaping).
    // Whole rows only: the caller cuts blocks on row boundaries.
    Status encode(const Chunk& chunk, std::string* out);

private:
    const TupleDescriptor* _tuple_desc;
    const std::string _column_separator;
    const std::string _null_marker;
    std::vector<std::unique_ptr<csv::Converter>> _converters; // per slot, non-nullable base
};

} // namespace starrocks::connector
