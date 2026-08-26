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
#include <string>

#include "connector/greenplum/gpfdist_server.h"
#include "connector/greenplum/gpfdist_session.h"

namespace starrocks::connector::gpfdist {

namespace {

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

    // -tA: tuples only, unaligned. -v ON_ERROR_STOP=1: fail loudly.
    std::string cmd = psql_bin + " -h " + host + " -p " + port + " -U " + user + " -d " + db +
                      " -tA -v ON_ERROR_STOP=1 -c \"" + sql + "\" 2>&1";
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

} // namespace starrocks::connector::gpfdist
