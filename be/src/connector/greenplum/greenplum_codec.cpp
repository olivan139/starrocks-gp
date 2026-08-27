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

#include "connector/greenplum/greenplum_codec.h"

#include "column/chunk.h"
#include "column/nullable_column.h"
#include "base/string/slice.h"
#include "fmt/format.h"

#include <cctype>
#include "formats/csv/converter.h"
#include "formats/io/formatted_output_stream_string.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"

// Greenplum/PostgreSQL COPY TEXT dialect.
//
// On OUTPUT the server escapes exactly: backslash ("\\"), newline ("\n" -
// the letter n), carriage return ("\r"), and the delimiter character
// (backslash + the raw character). NULL is the null marker (default "\N"),
// emitted UNescaped - which is why null detection must happen on the raw
// field bytes, before unescaping.
//
// On INPUT the server additionally accepts \b \f \t \v (we accept them too);
// octal/hex escapes are accepted by COPY but never produced by TEXT output,
// so we reject them loudly rather than half-support them.

namespace starrocks::connector {

namespace {

// Split-aware scan state: true if data[i] is preceded by an ODD number of
// backslashes (i.e. it is escaped).
size_t find_unescaped(std::string_view data, size_t from, char target) {
    bool escaped = false;
    for (size_t i = from; i < data.size(); ++i) {
        if (escaped) {
            escaped = false;
        } else if (data[i] == '\\') {
            escaped = true;
        } else if (data[i] == target) {
            return i;
        }
    }
    return std::string_view::npos;
}

// Greenplum's COPY ... TO (the writable-external-table wire format) renders a
// boolean column as the single characters "t"/"f" (Postgres boolout). StarRocks'
// CSV boolean converter only accepts "1"/"0"/"true"/"false", so a raw "t"/"f"
// would fail the whole query. Normalize GP's textual boolean spellings here, in
// the GP-dialect-aware codec, to "1"/"0" before the generic converter sees them.
// The full Postgres boolean input vocabulary is accepted defensively even though
// COPY output itself only ever emits t/f.
bool normalize_greenplum_bool(std::string* v) {
    auto eq_ci = [&](std::string_view lit) {
        if (v->size() != lit.size()) {
            return false;
        }
        for (size_t i = 0; i < lit.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>((*v)[i])) != lit[i]) {
                return false;
            }
        }
        return true;
    };
    if (eq_ci("t") || eq_ci("true") || eq_ci("y") || eq_ci("yes") || eq_ci("on") || eq_ci("1")) {
        v->assign("1");
        return true;
    }
    if (eq_ci("f") || eq_ci("false") || eq_ci("n") || eq_ci("no") || eq_ci("off") || eq_ci("0")) {
        v->assign("0");
        return true;
    }
    return false; // leave untouched; converter reports the parse error
}

Status unescape(std::string_view in, std::string* out) {
    out->clear();
    out->reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        char c = in[i];
        if (c != '\\') {
            out->push_back(c);
            continue;
        }
        if (++i >= in.size()) {
            return Status::Corruption("greenplum text row ends with a dangling backslash");
        }
        char e = in[i];
        switch (e) {
        case 'n':
            out->push_back('\n');
            break;
        case 'r':
            out->push_back('\r');
            break;
        case 't':
            out->push_back('\t');
            break;
        case 'b':
            out->push_back('\b');
            break;
        case 'f':
            out->push_back('\f');
            break;
        case 'v':
            out->push_back('\v');
            break;
        case 'x':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
            return Status::Corruption("octal/hex escapes are not supported in greenplum TEXT data");
        default:
            out->push_back(e); // covers "\\" and backslash+delimiter
        }
    }
    return Status::OK();
}

void escape_into(std::string_view in, char delimiter, std::string* out) {
    for (char c : in) {
        if (c == '\\') {
            out->append("\\\\");
        } else if (c == '\n') {
            out->append("\\n");
        } else if (c == '\r') {
            out->append("\\r");
        } else if (c == delimiter) {
            out->push_back('\\');
            out->push_back(c);
        } else {
            out->push_back(c);
        }
    }
}

} // namespace

GreenplumTextDecoder::GreenplumTextDecoder(const TupleDescriptor* tuple_desc, std::string column_separator,
                                           std::string null_marker)
        : _tuple_desc(tuple_desc),
          _column_separator(std::move(column_separator)),
          _null_marker(std::move(null_marker)) {
    for (const SlotDescriptor* slot : _tuple_desc->slots()) {
        _converters.push_back(csv::get_converter(slot->type(), /*nullable=*/false));
    }
}

GreenplumTextDecoder::~GreenplumTextDecoder() = default;

Status GreenplumTextDecoder::_decode_row(std::string_view row, Chunk* chunk) {
    const auto& slots = _tuple_desc->slots();
    const char sep = _column_separator.empty() ? '\t' : _column_separator[0];

    csv::Converter::Options options;
    // Correctness rule (contract doc): unparseable value fails the query,
    // never becomes NULL.
    options.invalid_field_as_null = false;

    size_t field_start = 0;
    for (size_t k = 0; k < slots.size(); ++k) {
        size_t field_end;
        if (k + 1 == slots.size()) {
            field_end = row.size();
            if (find_unescaped(row, field_start, sep) != std::string_view::npos) {
                return Status::Corruption(fmt::format("greenplum text row has more than {} fields", slots.size()));
            }
        } else {
            field_end = find_unescaped(row, field_start, sep);
            if (field_end == std::string_view::npos) {
                return Status::Corruption(fmt::format(
                        "greenplum text row has {} fields, expected {}: truncated after column '{}'", k + 1,
                        slots.size(), slots[k]->col_name()));
            }
        }
        std::string_view raw = row.substr(field_start, field_end - field_start);
        field_start = field_end + 1;

        const SlotDescriptor* slot = slots[k];
        Column* column = chunk->get_column_raw_ptr_by_index(chunk->get_index_by_slot_id(slot->id()));

        // NULL is decided on the RAW bytes: the marker itself arrives
        // unescaped, while literal data that *looks* like it arrives escaped
        // ("\\N") and therefore does not compare equal here.
        if (raw == _null_marker) {
            if (!column->is_nullable()) {
                return Status::Corruption(
                        fmt::format("NULL value for non-nullable column '{}'", slot->col_name()));
            }
            (void)down_cast<NullableColumn*>(column)->append_nulls(1);
            continue;
        }

        RETURN_IF_ERROR(unescape(raw, &_field_buf));
        if (slot->type().type == TYPE_BOOLEAN) {
            normalize_greenplum_bool(&_field_buf);
        }
        options.type_desc = &slot->type();

        Column* data_column = column;
        NullableColumn* nullable = nullptr;
        if (column->is_nullable()) {
            nullable = down_cast<NullableColumn*>(column);
            data_column = nullable->data_column_raw_ptr();
        }
        if (!_converters[k]->read_string(data_column, Slice(_field_buf), options)) {
            return Status::Corruption(fmt::format(
                    "cannot parse greenplum value for column '{}' (type {}): '{}'", slot->col_name(),
                    slot->type().debug_string(), _field_buf));
        }
        if (nullable != nullptr) {
            nullable->null_column_raw_ptr()->append(0);
        }
    }
    return Status::OK();
}

StatusOr<size_t> GreenplumTextDecoder::decode(std::string_view data, Chunk* chunk, int64_t* rows_out) {
    size_t consumed = 0;
    while (true) {
        // Rows are newline-terminated; GP TEXT output never emits a raw
        // newline inside a value (it becomes "\n"), but scan escape-aware
        // anyway so corrupt input fails instead of misparsing.
        size_t eol = find_unescaped(data, consumed, '\n');
        if (eol == std::string_view::npos) {
            return consumed; // tail is an incomplete row: caller re-offers it
        }
        std::string_view row = data.substr(consumed, eol - consumed);
        if (!row.empty() && row.back() == '\r') {
            row.remove_suffix(1);
        }
        if (!row.empty()) {
            // Keep the chunk consistent if a field mid-row fails: converters
            // append per-column as they go, so on error we truncate every
            // column back to the last complete row (same idiom as csv_scanner).
            size_t before = chunk->num_rows();
            if (Status st = _decode_row(row, chunk); !st.ok()) {
                chunk->set_num_rows(before);
                return st;
            }
            ++(*rows_out);
        }
        consumed = eol + 1;
    }
}

GreenplumTextEncoder::GreenplumTextEncoder(const TupleDescriptor* tuple_desc, std::string column_separator,
                                           std::string null_marker)
        : _tuple_desc(tuple_desc),
          _column_separator(std::move(column_separator)),
          _null_marker(std::move(null_marker)) {
    for (const SlotDescriptor* slot : _tuple_desc->slots()) {
        _converters.push_back(csv::get_converter(slot->type(), /*nullable=*/false));
    }
}

GreenplumTextEncoder::~GreenplumTextEncoder() = default;

Status GreenplumTextEncoder::encode(const Chunk& chunk, std::string* out) {
    const auto& slots = _tuple_desc->slots();
    const char sep = _column_separator.empty() ? '\t' : _column_separator[0];
    csv::Converter::Options options;

    for (size_t row = 0; row < chunk.num_rows(); ++row) {
        for (size_t k = 0; k < slots.size(); ++k) {
            if (k > 0) out->push_back(sep);
            const SlotDescriptor* slot = slots[k];
            const Column* column = chunk.get_column_raw_ptr_by_index(chunk.get_slot_id_to_index_map().at(slot->id()));

            const Column* data_column = column;
            if (column->is_nullable()) {
                const auto* nullable = down_cast<const NullableColumn*>(column);
                if (nullable->immutable_null_column_data()[row] != 0) {
                    out->append(_null_marker); // NULL marker goes out UNescaped
                    continue;
                }
                data_column = nullable->data_column_raw_ptr();
            }
            options.type_desc = &slot->type();
            formats::FormattedOutputStreamString scratch;
            RETURN_IF_ERROR(_converters[k]->write_string(&scratch, *data_column, row, options));
            RETURN_IF_ERROR(scratch.finalize());
            escape_into(scratch.as_string(), sep, out);
        }
        out->push_back('\n');
    }
    return Status::OK();
}

} // namespace starrocks::connector
