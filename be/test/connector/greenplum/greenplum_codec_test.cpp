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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/string/slice.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

namespace {

// TupleDescriptor construction idiom modeled on
// be/test/exec/connector_scan_node_test.cpp (a registered, exercised BE
// test): build TSlotDescriptor/TTupleDescriptor thrift structs via the
// TDescriptorTableBuilder/TTupleDescriptorBuilder/TSlotDescriptorBuilder
// helpers in runtime/descriptor_helper.h, then materialize real
// TupleDescriptor/SlotDescriptor objects through DescriptorTbl::create().
//
// Unlike connector_scan_node_test.cpp we pass a nullptr RuntimeState*: the
// same pattern is used in production by
// FragmentMgr::exec_plan_fragment/get_query_plan_info
// (be/src/runtime/fragment_mgr.cpp:845, `DescriptorTbl::create(nullptr,
// &obj_pool, ...)`). DescriptorTbl::create() only dereferences `state` to
// special-case the fragment-scoped MemPool
// (`state != nullptr && pool == state->obj_pool()`), so nullptr is safe for
// a standalone codec test that doesn't need a full RuntimeState/ExecEnv.
//
// NOTE: some other BE connector/exec tests (e.g.
// be/test/connector/mysql/mysql_connector_test.cpp) construct a
// TupleDescriptor via `pool->add(new TupleDescriptor(thrift_tuple))`
// directly; that does NOT compile as written because
// TupleDescriptor's constructors are private (only DescriptorTbl,
// OlapTableSchemaParam and ObjectPool are friends, and friendship does not
// extend to code that merely calls through ObjectPool::add). Those files
// are not registered in be/test/CMakeLists.txt, which is presumably why
// this has gone unnoticed. DescriptorTbl::create() is the real, working,
// friend-sanctioned way to build one from a thrift table.
const TupleDescriptor* build_tuple(ObjectPool* pool, const std::vector<std::pair<std::string, TypeDescriptor>>& cols) {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (const auto& [name, type] : cols) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(type).nullable(true).column_name(name);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* desc_tbl = nullptr;
    Status st = DescriptorTbl::create(nullptr, pool, desc_tbl_builder.desc_tbl(), &desc_tbl, 4096);
    if (!st.ok()) {
        ADD_FAILURE() << "failed to build TupleDescriptor: " << st;
        return nullptr;
    }
    return desc_tbl->get_tuple_descriptor(0);
}

// Build a Chunk with one column per slot, in slot order, matching the
// idiom in be/test/storage/chunk_helper_test.cpp
// (ColumnHelper::create_column(...) + Chunk::append_column(..., slot_id)).
ChunkPtr build_chunk(const TupleDescriptor* tuple_desc) {
    auto chunk = std::make_shared<Chunk>();
    for (const SlotDescriptor* slot : tuple_desc->slots()) {
        auto col = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        chunk->append_column(std::move(col), slot->id());
    }
    return chunk;
}

// Slot ids for the 5-column fixture tuple (id INT, name VARCHAR, dt DATE,
// price DECIMAL64(10,2), score DOUBLE); slot ids are assigned 0.. in
// declaration order by TTupleDescriptorBuilder::build().
enum WideSlot { kId = 0, kName = 1, kDt = 2, kPrice = 3, kScore = 4 };

// Slot ids for the 2-column fixture tuple (id INT, val VARCHAR), used by
// tests that don't need the full type variety.
enum NarrowSlot { kNId = 0, kNVal = 1 };

} // namespace

class GreenplumCodecTest : public ::testing::Test {
protected:
    const TupleDescriptor* wide_tuple() {
        return build_tuple(&_pool, {{"id", TypeDescriptor(TYPE_INT)},
                                    {"name", TypeDescriptor::create_varchar_type(64)},
                                    {"dt", TypeDescriptor(TYPE_DATE)},
                                    {"price", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2)},
                                    {"score", TypeDescriptor(TYPE_DOUBLE)}});
    }

    const TupleDescriptor* narrow_tuple() {
        return build_tuple(&_pool, {{"id", TypeDescriptor(TYPE_INT)}, {"val", TypeDescriptor::create_varchar_type(64)}});
    }

    ObjectPool _pool;
};

// ---------------------------------------------------------------------------
// decode_basic
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_basic) {
    const TupleDescriptor* tuple_desc = wide_tuple();
    ASSERT_NE(nullptr, tuple_desc);
    ChunkPtr chunk = build_chunk(tuple_desc);

    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;
    std::string data = "1\tapple\t2026-01-01\t3.14\t2.5\n";
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(data.size(), result.value());
    EXPECT_EQ(1, rows);
    ASSERT_EQ(1u, chunk->num_rows());

    EXPECT_EQ(1, chunk->get_column_by_slot_id(kId)->get(0).get_int32());
    EXPECT_EQ("apple", chunk->get_column_by_slot_id(kName)->get(0).get_slice().to_string());
    EXPECT_EQ("2026-01-01", chunk->get_column_by_slot_id(kDt)->get(0).get_date().to_string());
    // DECIMAL64(10,2): the underlying column stores the scaled int64 value.
    EXPECT_EQ(314, chunk->get_column_by_slot_id(kPrice)->get(0).get_int64());
    EXPECT_DOUBLE_EQ(2.5, chunk->get_column_by_slot_id(kScore)->get(0).get_double());
}

// ---------------------------------------------------------------------------
// decode_null
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_null) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    ChunkPtr chunk = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;

    // Row 1: val is exactly the null marker on the wire (raw bytes '\','N')
    //        -> NULL.
    // Row 2: val is an ESCAPED backslash followed by a literal 'N' on the
    //        wire (raw bytes '\','\','N') -> unescapes to the 2-char string
    //        "\N", and must NOT be treated as null: null detection happens
    //        on the raw field bytes, before unescaping.
    std::string data = "1\t\\N\n2\t\\\\N\n";
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(2, rows);
    ASSERT_EQ(2u, chunk->num_rows());

    const ColumnPtr& val = chunk->get_column_by_slot_id(kNVal);
    EXPECT_TRUE(val->is_null(0));
    ASSERT_FALSE(val->is_null(1));
    EXPECT_EQ("\\N", val->get(1).get_slice().to_string()); // literal backslash + 'N'
}

// ---------------------------------------------------------------------------
// decode_escapes
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_escapes) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    ChunkPtr chunk = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;

    // Row 1: val = "a" + <escaped delimiter, i.e. backslash + raw TAB> + "b"
    //        -> unescapes to "a\tb"; the embedded literal tab must not be
    //        mistaken for the field separator.
    // Row 2: val = "x" + <escaped newline, i.e. backslash + letter 'n'> +
    //        "y" -> unescapes to "x\ny"; the embedded literal newline must
    //        not be mistaken for the row terminator.
    std::string data = "1\ta\\\tb\n2\tx\\ny\n";
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(2, rows);
    ASSERT_EQ(2u, chunk->num_rows());

    const ColumnPtr& val = chunk->get_column_by_slot_id(kNVal);
    EXPECT_EQ("a\tb", val->get(0).get_slice().to_string());
    EXPECT_EQ("x\ny", val->get(1).get_slice().to_string());
}

// ---------------------------------------------------------------------------
// decode_partial_row
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_partial_row) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    ChunkPtr chunk = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;

    std::string data = "1\tapple\n2\tpea"; // second row has no trailing newline yet
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_TRUE(result.ok()) << result.status();
    const size_t first_row_len = std::string("1\tapple\n").size();
    EXPECT_EQ(first_row_len, result.value());
    EXPECT_EQ(1, rows);
    ASSERT_EQ(1u, chunk->num_rows());
    EXPECT_EQ(1, chunk->get_column_by_slot_id(kNId)->get(0).get_int32());
    EXPECT_EQ("apple", chunk->get_column_by_slot_id(kNVal)->get(0).get_slice().to_string());

    // The caller re-offers the unconsumed tail together with the rest of
    // the row from the next slab.
    std::string tail = data.substr(result.value()) + "r\n";
    EXPECT_EQ("2\tpear\n", tail);
    auto result2 = decoder.decode(tail, chunk.get(), &rows);
    ASSERT_TRUE(result2.ok()) << result2.status();
    EXPECT_EQ(tail.size(), result2.value());
    EXPECT_EQ(2, rows);
    ASSERT_EQ(2u, chunk->num_rows());
    EXPECT_EQ(2, chunk->get_column_by_slot_id(kNId)->get(1).get_int32());
    EXPECT_EQ("pear", chunk->get_column_by_slot_id(kNVal)->get(1).get_slice().to_string());
}

// ---------------------------------------------------------------------------
// decode_bad_value_fails
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_bad_value_fails) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    ChunkPtr chunk = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;

    std::string data = "notanint\tfoo\n";
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(0, rows);
}

// On a mid-row parse failure the decoder rolls the chunk back to the last
// COMPLETE row (via set_num_rows), so every column keeps a consistent row
// count even though some fields of the failing row were already appended.
TEST_F(GreenplumCodecTest, decode_bad_value_mid_row_rolls_chunk_back) {
    const TupleDescriptor* tuple_desc = wide_tuple();
    ChunkPtr chunk = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;

    // A first good row, then a row whose dt (3rd of 5 columns) fails to parse.
    std::string data = "9\tgood\t2026-02-02\t1.00\t9.9\n1\tapple\tnotadate\t3.14\t2.5\n";
    auto result = decoder.decode(data, chunk.get(), &rows);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_corruption()) << result.status();
    EXPECT_EQ(1, rows); // the first good row was counted

    // Every column is truncated back to exactly the one complete row: no raggedness.
    EXPECT_EQ(1u, chunk->num_rows());
    EXPECT_EQ(1u, chunk->get_column_by_slot_id(kId)->size());
    EXPECT_EQ(1u, chunk->get_column_by_slot_id(kName)->size());
    EXPECT_EQ(1u, chunk->get_column_by_slot_id(kDt)->size());
    EXPECT_EQ(1u, chunk->get_column_by_slot_id(kPrice)->size());
    EXPECT_EQ(1u, chunk->get_column_by_slot_id(kScore)->size());
}

// ---------------------------------------------------------------------------
// decode_wrong_field_count_fails
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, decode_wrong_field_count_fails) {
    const TupleDescriptor* tuple_desc = wide_tuple();
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");

    {
        ChunkPtr chunk = build_chunk(tuple_desc);
        int64_t rows = 0;
        std::string too_few = "1\tapple\n"; // only 2 of 5 fields
        auto result = decoder.decode(too_few, chunk.get(), &rows);
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
    }
    {
        ChunkPtr chunk = build_chunk(tuple_desc);
        int64_t rows = 0;
        std::string too_many = "1\tapple\t2026-01-01\t3.14\t2.5\textra\n"; // 6 fields
        auto result = decoder.decode(too_many, chunk.get(), &rows);
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_corruption()) << result.status();
    }
}

// ---------------------------------------------------------------------------
// encode_roundtrip
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, encode_roundtrip) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    const auto& slots = tuple_desc->slots();

    // Chunk::get_column_by_slot_id() returns a Column::Ptr (an
    // ImmutPtr<Column>, see be/src/common/cow.h), which only exposes
    // const access - by design, columns already inserted into a Chunk
    // can't be mutated through that accessor from outside Chunk/production
    // code that knows to use get_column_raw_ptr_by_index() instead (as
    // GreenplumTextDecoder does). So build and populate the
    // MutableColumnPtr columns first, then move them into the chunk.
    auto id_col = ColumnHelper::create_column(slots[kNId]->type(), slots[kNId]->is_nullable());
    auto val_col = ColumnHelper::create_column(slots[kNVal]->type(), slots[kNVal]->is_nullable());

    // Row 0: val contains both a delimiter (tab) and a backslash - both
    // must survive escape-on-encode/unescape-on-decode.
    id_col->append_datum(int32_t{1});
    val_col->append_datum(Slice("a\tb\\c"));
    // Row 1: val is NULL.
    id_col->append_datum(int32_t{2});
    val_col->append_datum(Datum());

    ChunkPtr src = std::make_shared<Chunk>();
    src->append_column(std::move(id_col), slots[kNId]->id());
    src->append_column(std::move(val_col), slots[kNVal]->id());
    ASSERT_EQ(2u, src->num_rows());

    GreenplumTextEncoder encoder(tuple_desc, "\t", "\\N");
    std::string wire;
    ASSERT_TRUE(encoder.encode(*src, &wire).ok());

    ChunkPtr dst = build_chunk(tuple_desc);
    GreenplumTextDecoder decoder(tuple_desc, "\t", "\\N");
    int64_t rows = 0;
    auto result = decoder.decode(wire, dst.get(), &rows);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(wire.size(), result.value());
    EXPECT_EQ(2, rows);
    ASSERT_EQ(2u, dst->num_rows());

    const ColumnPtr& src_id = src->get_column_by_slot_id(kNId);
    const ColumnPtr& src_val = src->get_column_by_slot_id(kNVal);
    const ColumnPtr& dst_id = dst->get_column_by_slot_id(kNId);
    const ColumnPtr& dst_val = dst->get_column_by_slot_id(kNVal);
    for (size_t row = 0; row < 2; ++row) {
        SCOPED_TRACE(row);
        EXPECT_EQ(src_id->get(row).get_int32(), dst_id->get(row).get_int32());
        EXPECT_EQ(src_val->is_null(row), dst_val->is_null(row));
        if (!src_val->is_null(row)) {
            EXPECT_EQ(src_val->get(row).get_slice().to_string(), dst_val->get(row).get_slice().to_string());
        }
    }
}

// ---------------------------------------------------------------------------
// encode_null_marker
// ---------------------------------------------------------------------------
TEST_F(GreenplumCodecTest, encode_null_marker) {
    const TupleDescriptor* tuple_desc = narrow_tuple();
    const auto& slots = tuple_desc->slots();

    // See encode_roundtrip above for why the columns are populated before
    // being moved into the chunk.
    auto id_col = ColumnHelper::create_column(slots[kNId]->type(), slots[kNId]->is_nullable());
    auto val_col = ColumnHelper::create_column(slots[kNVal]->type(), slots[kNVal]->is_nullable());
    id_col->append_datum(int32_t{5});
    val_col->append_datum(Datum());

    ChunkPtr chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(id_col), slots[kNId]->id());
    chunk->append_column(std::move(val_col), slots[kNVal]->id());
    ASSERT_EQ(1u, chunk->num_rows());

    GreenplumTextEncoder encoder(tuple_desc, "\t", "\\N");
    std::string wire;
    ASSERT_TRUE(encoder.encode(*chunk, &wire).ok());
    // The null marker goes out verbatim/unescaped.
    EXPECT_EQ("5\t\\N\n", wire);
}

} // namespace starrocks::connector
