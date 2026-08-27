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

// Real-Greenplum integration test for the gpfdist server: drives an actual
// Greenplum cluster (its real url_curl.c segment client) against our server,
// in both directions. Env-guarded, so it is skipped in normal CI and only
// runs when GREENPLUM_IT_HOST is set:
//
//   docker run -d --name gp-it -p 6432:5432 andruche/greenplum:6
//   GREENPLUM_IT_HOST=127.0.0.1 GREENPLUM_IT_PORT=6432 \
//   GREENPLUM_IT_CALLBACK=host.docker.internal GREENPLUM_IT_GPFDIST_PORT=8907 \
//   BUILD_TYPE=RELEASE ./run-be-ut.sh --build-target starrocks_test \
//       --gtest_filter='GpfdistGreenplumIT*'
//
// The callback host is what GP segments use to reach this server; from a
// Docker GP it is host.docker.internal.

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "connector/greenplum/greenplum_codec.h"
#include "connector/greenplum/gpfdist_server.h"
#include "connector/greenplum/gpfdist_session.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "types/date_value.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

namespace starrocks::connector::gpfdist {

namespace {

// Same friend-sanctioned TupleDescriptor construction idiom used by
// greenplum_codec_test.cpp; duplicated here (test-only) so the integration
// test can exercise the real GreenplumTextEncoder/Decoder against live GP.
const TupleDescriptor* build_tuple(ObjectPool* pool,
                                   const std::vector<std::pair<std::string, TypeDescriptor>>& cols) {
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

ChunkPtr build_chunk(const TupleDescriptor* tuple_desc) {
    auto chunk = std::make_shared<Chunk>();
    for (const SlotDescriptor* slot : tuple_desc->slots()) {
        chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
    }
    return chunk;
}

std::string env_or(const char* k, const char* def) {
    const char* v = std::getenv(k);
    return v != nullptr && *v != '\0' ? std::string(v) : std::string(def);
}

// Run a psql command against the Greenplum master; returns stdout (trimmed).
// Uses the host libpq psql; PGPASSWORD is exported for the process.
std::string psql(const std::string& sql) {
    std::string host = env_or("GREENPLUM_IT_HOST", "127.0.0.1");
    std::string port = env_or("GREENPLUM_IT_PORT", "6432");
    std::string user = env_or("GREENPLUM_IT_USER", "gpadmin");
    std::string db = env_or("GREENPLUM_IT_DB", "postgres");
    std::string psql_bin = env_or("GREENPLUM_IT_PSQL", "/opt/homebrew/opt/libpq/bin/psql");
    setenv("PGPASSWORD", env_or("GREENPLUM_IT_PASSWORD", "gpadmin").c_str(), 1);

    // Pass the SQL through an environment variable rather than inlining it into
    // the shell command: a double-quoted "$VAR" expansion is NOT re-scanned for
    // backslash escapes, whereas a literal "...\\N..." inside double quotes has
    // its "\\" collapsed to "\" by /bin/sh. That collapsing would turn a
    // deliberate E'\\N' null marker into E'\N' (which Postgres then reads as a
    // bare "N"), silently breaking NULL round-tripping in these tests.
    setenv("STARROCKS_GP_SQL", sql.c_str(), 1);

    // -tA: tuples only, unaligned. -v ON_ERROR_STOP=1: fail loudly.
    std::string cmd = psql_bin + " -h " + host + " -p " + port + " -U " + user + " -d " + db +
                      " -tA -v ON_ERROR_STOP=1 -c \"$STARROCKS_GP_SQL\" 2>&1";
    std::array<char, 4096> buf{};
    std::string out;
    FILE* pipe = popen(cmd.c_str(), "r");
    EXPECT_NE(nullptr, pipe) << "popen failed for: " << cmd;
    if (pipe == nullptr) return out;
    while (fgets(buf.data(), buf.size(), pipe) != nullptr) out += buf.data();
    pclose(pipe);
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
    return out;
}

} // namespace

class GpfdistGreenplumIT : public ::testing::Test {
protected:
    void SetUp() override {
        if (std::getenv("GREENPLUM_IT_HOST") == nullptr) {
            GTEST_SKIP() << "GREENPLUM_IT_HOST not set; skipping real-Greenplum integration test";
        }
        _port = std::atoi(env_or("GREENPLUM_IT_GPFDIST_PORT", "8907").c_str());
        _callback = env_or("GREENPLUM_IT_CALLBACK", "host.docker.internal");
        ASSERT_TRUE(GpfdistServer::start_once(_port).ok());
    }

    std::string location(const std::string& token) const {
        return "gpfdist://" + _callback + ":" + std::to_string(_port) + "/" + token;
    }

    int _port = 8907;
    std::string _callback;
};

// WRITE direction (StarRocks -> Greenplum): our server SERVES rows, GP segments
// pull them into a real table via a READABLE external table.
TEST_F(GpfdistGreenplumIT, write_serves_rows_into_greenplum) {
    const std::string token = "it_write";
    auto session = SessionRegistry::instance()->create_pull(token, 1 << 20);
    // Two whole rows, TEXT format (tab-delimited), pre-staged (as the sink would).
    ASSERT_TRUE(session->put_block("1\tapple\n2\tpear\n", 2000).ok());
    ASSERT_TRUE(session->put_block("3\tplum\n", 2000).ok());
    session->finish();

    psql("DROP EXTERNAL TABLE IF EXISTS ext_w_it");
    psql("DROP TABLE IF EXISTS gp_w_it");
    psql("CREATE TABLE gp_w_it (id int, name text) DISTRIBUTED BY (id)");
    ASSERT_EQ("CREATE EXTERNAL TABLE",
              psql("CREATE READABLE EXTERNAL TABLE ext_w_it (id int, name text) "
                   "LOCATION('" + location(token) + "') FORMAT 'TEXT' (DELIMITER E'\\t')"));
    // GP segments pull from our server and populate the real table.
    psql("INSERT INTO gp_w_it SELECT * FROM ext_w_it");

    EXPECT_EQ("3", psql("SELECT count(*) FROM gp_w_it"));
    EXPECT_EQ("apple", psql("SELECT name FROM gp_w_it WHERE id=1"));
    EXPECT_EQ("pear", psql("SELECT name FROM gp_w_it WHERE id=2"));
    EXPECT_EQ("plum", psql("SELECT name FROM gp_w_it WHERE id=3"));

    psql("DROP EXTERNAL TABLE IF EXISTS ext_w_it");
    psql("DROP TABLE IF EXISTS gp_w_it");
    SessionRegistry::instance()->remove(token, 0);
}

// READ direction (Greenplum -> StarRocks): GP segments PUSH a source table's
// rows into our server via a WRITABLE external table; we drain and verify.
TEST_F(GpfdistGreenplumIT, read_receives_rows_from_greenplum) {
    const std::string token = "it_read";
    auto session = SessionRegistry::instance()->create_push(token, 1 << 20);

    psql("DROP EXTERNAL TABLE IF EXISTS ext_r_it");
    psql("DROP TABLE IF EXISTS gp_r_it");
    psql("CREATE TABLE gp_r_it (id int, name text) DISTRIBUTED BY (id)");
    psql("INSERT INTO gp_r_it VALUES (10,'red'),(20,'green'),(30,'blue')");
    ASSERT_EQ("CREATE EXTERNAL TABLE",
              psql("CREATE WRITABLE EXTERNAL TABLE ext_r_it (id int, name text) "
                   "LOCATION('" + location(token) + "') FORMAT 'TEXT' (DELIMITER E'\\t') "
                   "DISTRIBUTED BY (id)"));
    // Segments push the source rows to our server; the session buffers them.
    psql("INSERT INTO ext_r_it SELECT * FROM gp_r_it");

    // Drain everything the segments pushed (all segments have sent DONE).
    std::string received;
    while (true) {
        auto slab = session->take(5000);
        ASSERT_TRUE(slab.ok()) << slab.status();
        if (!slab->has_value()) break;
        received += **slab;
    }
    // 3 rows, tab-delimited, newline-terminated (order across segments varies).
    int lines = 0;
    for (char c : received) lines += (c == '\n');
    EXPECT_EQ(3, lines) << "received: " << received;
    EXPECT_NE(std::string::npos, received.find("10\tred\n"));
    EXPECT_NE(std::string::npos, received.find("20\tgreen\n"));
    EXPECT_NE(std::string::npos, received.find("30\tblue\n"));

    psql("DROP EXTERNAL TABLE IF EXISTS ext_r_it");
    psql("DROP TABLE IF EXISTS gp_r_it");
    SessionRegistry::instance()->remove(token, 0);
}

// READ direction, HARD cases: hard types (boolean, decimal, date, double),
// special characters (embedded tab / backslash / newline / empty / the literal
// null-marker string) and NULLs -- all pushed from a real GP through the REAL
// GreenplumTextDecoder, mirroring exactly what GreenplumReadOrchestrator does
// (typed WRITABLE ext table + DISTRIBUTED RANDOMLY + the DateStyle /
// extra_float_digits session GUCs). This is the test that exercises:
//   * boolean rendered by GP as t/f (must decode to 1/0, not fail the query),
//   * double round-trip precision (extra_float_digits=3),
//   * date/timestamp format stability (DateStyle=ISO),
//   * TEXT escaping produced by GP's own COPY output.
TEST_F(GpfdistGreenplumIT, read_hard_types_and_escaping_from_greenplum) {
    const std::string token = "it_read_hard";
    auto session = SessionRegistry::instance()->create_push(token, 4 << 20);

    ObjectPool pool;
    const TupleDescriptor* tuple = build_tuple(
            &pool, {{"id", TypeDescriptor(TYPE_INT)},
                    {"flag", TypeDescriptor(TYPE_BOOLEAN)},
                    {"txt", TypeDescriptor::create_varchar_type(256)},
                    {"amt", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 12, 2)},
                    {"dt", TypeDescriptor(TYPE_DATE)},
                    {"score", TypeDescriptor(TYPE_DOUBLE)}});
    ASSERT_NE(nullptr, tuple);

    psql("DROP EXTERNAL TABLE IF EXISTS ext_rh_it");
    psql("DROP TABLE IF EXISTS gp_rh_it");
    psql("CREATE TABLE gp_rh_it (id int, flag boolean, txt text, amt numeric(12,2), dt date, "
         "score double precision) DISTRIBUTED BY (id)");
    // Special characters are constructed server-side via chr() to avoid shell
    // quoting: chr(9)=TAB, chr(10)=NL, chr(92)=backslash.
    psql("INSERT INTO gp_rh_it VALUES "
         "(1, true,  'plain',                        123.45,   '2024-03-15', 0.1),"
         "(2, false, 'a'||chr(9)||'b',               -0.01,    '2000-01-01', 2.718281828459045),"
         "(3, NULL,  'back'||chr(92)||'slash',       0.00,     '1999-12-31', -1.5),"
         "(4, true,  'line'||chr(10)||'break',       999999.99,'2024-02-29', 1e-10),"
         "(5, false, '',                             0.00,     '2024-01-01', 0.0),"
         "(6, true,  chr(92)||'N',                   1.00,     '2024-06-01', 3.14159265358979)");
    // Mirror GreenplumReadOrchestrator exactly.
    psql("SET DateStyle TO 'ISO, YMD'");
    psql("SET extra_float_digits TO 3");
    psql("SET TimeZone TO 'UTC'");
    ASSERT_EQ("CREATE EXTERNAL TABLE",
              psql("CREATE WRITABLE EXTERNAL TABLE ext_rh_it "
                   "(id integer, flag boolean, txt text, amt numeric, dt date, score double precision) "
                   // The null marker must reach the server as SQL E'\\N' (a
                   // backslash then N) so GP's marker is the 2-char "\N" the
                   // codec uses. Writing E'\N' here would let PostgreSQL's
                   // escape-string parser collapse the unrecognized \N to a bare
                   // "N", silently breaking NULL round-tripping. This mirrors
                   // GreenplumReadOrchestrator.escape(), which doubles the slash.
                   "LOCATION('" + location(token) + "') FORMAT 'TEXT' (DELIMITER E'\\t' NULL E'\\\\N') "
                   "DISTRIBUTED RANDOMLY"));
    // Re-apply GUCs on the connection that runs the pushing INSERT (psql opens a
    // fresh connection per -c invocation), then push.
    psql("SET DateStyle TO 'ISO, YMD'; SET extra_float_digits TO 3; SET TimeZone TO 'UTC'; "
         "INSERT INTO ext_rh_it SELECT * FROM gp_rh_it");

    // Drain and decode with the real codec.
    ChunkPtr chunk = build_chunk(tuple);
    GreenplumTextDecoder decoder(tuple, "\t", "\\N");
    std::string pending;
    int64_t rows = 0;
    while (true) {
        auto slab = session->take(5000);
        ASSERT_TRUE(slab.ok()) << slab.status();
        if (!slab->has_value()) break;
        pending += **slab;
        auto consumed = decoder.decode(pending, chunk.get(), &rows);
        ASSERT_TRUE(consumed.ok()) << consumed.status();
        pending.erase(0, consumed.value());
    }
    EXPECT_TRUE(pending.empty()) << "undecoded tail: " << pending;
    ASSERT_EQ(6, rows);
    ASSERT_EQ(6u, chunk->num_rows());

    // Rows can arrive in any segment order; index by id.
    std::map<int32_t, size_t> byId;
    const ColumnPtr& idc = chunk->get_column_by_slot_id(0);
    for (size_t r = 0; r < chunk->num_rows(); ++r) byId[idc->get(r).get_int32()] = r;
    ASSERT_EQ(6u, byId.size());

    const ColumnPtr& flag = chunk->get_column_by_slot_id(1);
    const ColumnPtr& txt = chunk->get_column_by_slot_id(2);
    const ColumnPtr& amt = chunk->get_column_by_slot_id(3);
    const ColumnPtr& dt = chunk->get_column_by_slot_id(4);
    const ColumnPtr& score = chunk->get_column_by_slot_id(5);

    // boolean t/f  -> 1/0, NULL preserved (the fix under test)
    EXPECT_EQ(1, flag->get(byId[1]).get_int8());
    EXPECT_EQ(0, flag->get(byId[2]).get_int8());
    EXPECT_TRUE(flag->is_null(byId[3]));
    EXPECT_EQ(1, flag->get(byId[4]).get_int8());
    EXPECT_EQ(0, flag->get(byId[5]).get_int8());

    // special characters survive GP's escaping -> our unescape
    EXPECT_EQ("plain", txt->get(byId[1]).get_slice().to_string());
    EXPECT_EQ("a\tb", txt->get(byId[2]).get_slice().to_string());
    EXPECT_EQ("back\\slash", txt->get(byId[3]).get_slice().to_string());
    EXPECT_EQ("line\nbreak", txt->get(byId[4]).get_slice().to_string());
    EXPECT_EQ("", txt->get(byId[5]).get_slice().to_string());
    EXPECT_EQ("\\N", txt->get(byId[6]).get_slice().to_string()); // literal backslash+N, NOT null

    // DECIMAL64(12,2): stored as value*100
    EXPECT_EQ(12345, amt->get(byId[1]).get_int64());
    EXPECT_EQ(-1, amt->get(byId[2]).get_int64());
    EXPECT_EQ(99999999, amt->get(byId[4]).get_int64());

    // dates parse under pinned ISO DateStyle
    EXPECT_EQ("2024-03-15", dt->get(byId[1]).get_date().to_string());
    EXPECT_EQ("2024-02-29", dt->get(byId[4]).get_date().to_string());

    // doubles round-trip exactly thanks to extra_float_digits=3
    EXPECT_DOUBLE_EQ(0.1, score->get(byId[1]).get_double());
    EXPECT_DOUBLE_EQ(2.718281828459045, score->get(byId[2]).get_double());
    EXPECT_DOUBLE_EQ(1e-10, score->get(byId[4]).get_double());

    psql("DROP EXTERNAL TABLE IF EXISTS ext_rh_it");
    psql("DROP TABLE IF EXISTS gp_rh_it");
    SessionRegistry::instance()->remove(token, 0);
}

// WRITE direction, HARD cases: build a Chunk with hard types + special
// characters, run it through the REAL GreenplumTextEncoder, serve it, and let a
// real GP pull it into a typed table via the exact READABLE-ext DDL the write
// orchestrator emits (LIKE target). Verifies our encoder output is accepted and
// stored correctly by GP for every type, and that escaping survives.
TEST_F(GpfdistGreenplumIT, write_hard_types_and_escaping_into_greenplum) {
    const std::string token = "it_write_hard";

    ObjectPool pool;
    const TupleDescriptor* tuple = build_tuple(
            &pool, {{"id", TypeDescriptor(TYPE_INT)},
                    {"flag", TypeDescriptor(TYPE_BOOLEAN)},
                    {"txt", TypeDescriptor::create_varchar_type(256)},
                    {"amt", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 12, 2)},
                    {"dt", TypeDescriptor(TYPE_DATE)}});
    ASSERT_NE(nullptr, tuple);
    const auto& slots = tuple->slots();

    auto id_col = ColumnHelper::create_column(slots[0]->type(), true);
    auto flag_col = ColumnHelper::create_column(slots[1]->type(), true);
    auto txt_col = ColumnHelper::create_column(slots[2]->type(), true);
    auto amt_col = ColumnHelper::create_column(slots[3]->type(), true);
    auto dt_col = ColumnHelper::create_column(slots[4]->type(), true);
    auto add = [&](int32_t id, uint8_t flag, bool flag_null, const std::string& txt, int64_t amt_scaled,
                   const std::string& date) {
        id_col->append_datum(Datum(id));
        if (flag_null) {
            flag_col->append_datum(Datum());
        } else {
            flag_col->append_datum(Datum(flag));
        }
        txt_col->append_datum(Datum(Slice(txt)));
        amt_col->append_datum(Datum(int64_t{amt_scaled}));
        DateValue dv;
        ASSERT_TRUE(dv.from_string(date.c_str(), date.size()));
        dt_col->append_datum(Datum(dv));
    };
    add(1, 1, false, "plain", 12345, "2024-03-15");
    add(2, 0, false, "a\tb\\c", -1, "2000-01-01"); // embedded tab + backslash
    add(3, 0, true, "line\nbreak", 0, "1999-12-31"); // NULL flag + embedded newline
    add(4, 1, false, "\\N", 100, "2024-06-01"); // literal backslash+N must NOT become NULL

    ChunkPtr chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(id_col), slots[0]->id());
    chunk->append_column(std::move(flag_col), slots[1]->id());
    chunk->append_column(std::move(txt_col), slots[2]->id());
    chunk->append_column(std::move(amt_col), slots[3]->id());
    chunk->append_column(std::move(dt_col), slots[4]->id());
    ASSERT_EQ(4u, chunk->num_rows());

    auto session = SessionRegistry::instance()->create_pull(token, 4 << 20);
    GreenplumTextEncoder encoder(tuple, "\t", "\\N");
    std::string wire;
    ASSERT_TRUE(encoder.encode(*chunk, &wire).ok());
    ASSERT_TRUE(session->put_block(wire, 2000).ok());
    session->finish();

    psql("DROP EXTERNAL TABLE IF EXISTS ext_wh_it");
    psql("DROP TABLE IF EXISTS gp_wh_it");
    psql("CREATE TABLE gp_wh_it (id int, flag boolean, txt text, amt numeric(12,2), dt date) "
         "DISTRIBUTED BY (id)");
    // Exact write-orchestrator DDL shape: READABLE ext (LIKE target).
    ASSERT_EQ("CREATE EXTERNAL TABLE",
              psql("CREATE READABLE EXTERNAL TABLE ext_wh_it (LIKE gp_wh_it) "
                   // E'\\\\N' -> SQL E'\\N' -> GP null marker "\N" (a single
                   // backslash would be mangled to "N"; see the read test).
                   "LOCATION('" + location(token) + "') FORMAT 'TEXT' (DELIMITER E'\\t' NULL E'\\\\N')"));
    psql("INSERT INTO gp_wh_it SELECT * FROM ext_wh_it");

    EXPECT_EQ("4", psql("SELECT count(*) FROM gp_wh_it"));
    // Postgres/GP cast boolean->text as "true"/"false" (the "t"/"f" short form
    // is only the default display via boolout, not the ::text cast).
    EXPECT_EQ("true", psql("SELECT flag::text FROM gp_wh_it WHERE id=1"));
    EXPECT_EQ("false", psql("SELECT flag::text FROM gp_wh_it WHERE id=2"));
    EXPECT_EQ("", psql("SELECT coalesce(flag::text,'') FROM gp_wh_it WHERE id=3")); // NULL
    // GP stored the exact bytes: compare against server-side chr() expressions.
    EXPECT_EQ("1", psql("SELECT (txt = 'a'||chr(9)||'b'||chr(92)||'c')::int FROM gp_wh_it WHERE id=2"));
    EXPECT_EQ("1", psql("SELECT (txt = 'line'||chr(10)||'break')::int FROM gp_wh_it WHERE id=3"));
    EXPECT_EQ("1", psql("SELECT (txt = chr(92)||'N')::int FROM gp_wh_it WHERE id=4"));
    EXPECT_EQ("123.45", psql("SELECT amt::text FROM gp_wh_it WHERE id=1"));
    EXPECT_EQ("2024-03-15", psql("SELECT dt::text FROM gp_wh_it WHERE id=1"));

    psql("DROP EXTERNAL TABLE IF EXISTS ext_wh_it");
    psql("DROP TABLE IF EXISTS gp_wh_it");
    SessionRegistry::instance()->remove(token, 0);
}

// VOLUME: many rows across multiple blocks, verifying no rows are lost or
// duplicated end-to-end (write direction, real GP pulls).
TEST_F(GpfdistGreenplumIT, write_volume_into_greenplum) {
    const std::string token = "it_write_vol";
    const int kRows = 5000;
    auto session = SessionRegistry::instance()->create_pull(token, 16 << 20);
    std::string block;
    for (int i = 0; i < kRows; ++i) {
        block += std::to_string(i);
        block += "\tv";
        block += std::to_string(i);
        block += "\n";
        if (block.size() > (64u << 10)) { // flush in ~64KB blocks -> many blocks
            ASSERT_TRUE(session->put_block(block, 5000).ok());
            block.clear();
        }
    }
    if (!block.empty()) ASSERT_TRUE(session->put_block(block, 5000).ok());
    session->finish();

    psql("DROP EXTERNAL TABLE IF EXISTS ext_wv_it");
    psql("DROP TABLE IF EXISTS gp_wv_it");
    psql("CREATE TABLE gp_wv_it (id int, v text) DISTRIBUTED BY (id)");
    ASSERT_EQ("CREATE EXTERNAL TABLE",
              psql("CREATE READABLE EXTERNAL TABLE ext_wv_it (LIKE gp_wv_it) "
                   // E'\\\\N' -> SQL E'\\N' -> GP null marker "\N" (a single
                   // backslash would be mangled to "N"; see the read test).
                   "LOCATION('" + location(token) + "') FORMAT 'TEXT' (DELIMITER E'\\t' NULL E'\\\\N')"));
    psql("INSERT INTO gp_wv_it SELECT * FROM ext_wv_it");

    EXPECT_EQ(std::to_string(kRows), psql("SELECT count(*) FROM gp_wv_it"));
    EXPECT_EQ(std::to_string(kRows), psql("SELECT count(DISTINCT id) FROM gp_wv_it"));
    EXPECT_EQ("v4999", psql("SELECT v FROM gp_wv_it WHERE id=4999"));

    psql("DROP EXTERNAL TABLE IF EXISTS ext_wv_it");
    psql("DROP TABLE IF EXISTS gp_wv_it");
    SessionRegistry::instance()->remove(token, 0);
}

} // namespace starrocks::connector::gpfdist
