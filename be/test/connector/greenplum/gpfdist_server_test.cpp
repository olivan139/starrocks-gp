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

// End-to-end socket test of the gpfdist server: an in-process TCP client plays
// the Greenplum segment side (the url_curl.c contract) against the real
// listener, exercising both directions and the dedupe rule.

#include "connector/greenplum/gpfdist_server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <thread>

#include "connector/greenplum/gpfdist_protocol.h"
#include "connector/greenplum/gpfdist_session.h"

namespace starrocks::connector::gpfdist {

namespace {

int connect_local(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    EXPECT_EQ(0, ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)));
    return fd;
}

void send_all(int fd, const std::string& s) {
    size_t off = 0;
    while (off < s.size()) {
        ssize_t n = ::send(fd, s.data() + off, s.size() - off, 0);
        ASSERT_GT(n, 0);
        off += n;
    }
}

std::string recv_all(int fd) {
    std::string out;
    char buf[4096];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        out.append(buf, n);
    }
    return out;
}

std::string base_headers(int seg, int proto) {
    return "X-GP-XID: 1\r\nX-GP-CID: 1\r\nX-GP-SN: 1\r\nX-GP-PROTO: " + std::to_string(proto) +
           "\r\nX-GP-SEGMENT-ID: " + std::to_string(seg) + "\r\nX-GP-SEGMENT-COUNT: 1\r\n";
}

int post(int port, const std::string& token, int seg, const std::string& extra, const std::string& body) {
    int fd = connect_local(port);
    std::string req = "POST /" + token + " HTTP/1.0\r\n" + base_headers(seg, 0) + extra +
                      "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;
    send_all(fd, req);
    std::string resp = recv_all(fd);
    ::close(fd);
    return std::atoi(resp.substr(9, 3).c_str()); // "HTTP/1.0 XXX ..."
}

} // namespace

class GpfdistServerTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() { ASSERT_TRUE(GpfdistServer::start_once(0).ok()); }
    static void TearDownTestSuite() { GpfdistServer::shutdown(); }
    int port() { return GpfdistServer::bound_port(); }
};

// READ direction: segment POSTs rows (with a duplicate resend) -> PushSession
// must contain each row exactly once, then EOF.
TEST_F(GpfdistServerTest, post_push_with_dedup) {
    const std::string token = "e2e_push";
    auto session = SessionRegistry::instance()->create_push(token, 1 << 20);

    EXPECT_EQ(200, post(port(), token, 0, "X-GP-SEQ: 1\r\n", ""));                 // open
    EXPECT_EQ(200, post(port(), token, 0, "X-GP-SEQ: 2\r\n", "1\tapple\n"));       // data
    EXPECT_EQ(200, post(port(), token, 0, "X-GP-SEQ: 2\r\n", "1\tapple\n"));       // DUP resend
    EXPECT_EQ(200, post(port(), token, 0, "X-GP-DONE: 1\r\n", ""));               // done

    std::string all;
    while (true) {
        auto slab = session->take(2000);
        ASSERT_TRUE(slab.ok());
        if (!slab->has_value()) break;
        all += **slab;
    }
    EXPECT_EQ("1\tapple\n", all); // exactly once: the duplicate was dropped
    EXPECT_TRUE(session->all_segments_done());
    SessionRegistry::instance()->remove(token, 0);
}

// WRITE direction: sink puts blocks -> segment GET receives framed data + EOF.
TEST_F(GpfdistServerTest, get_pull_frames) {
    const std::string token = "e2e_pull";
    auto session = SessionRegistry::instance()->create_pull(token, 1 << 20);
    ASSERT_TRUE(session->put_block("10\tred\n", 1000).ok());
    ASSERT_TRUE(session->put_block("11\tblue\n", 1000).ok());
    session->finish();

    int fd = connect_local(port());
    send_all(fd, "GET /" + token + " HTTP/1.0\r\n" + base_headers(0, 1) + "\r\n");
    std::string resp = recv_all(fd);
    ::close(fd);

    ASSERT_EQ(0u, resp.rfind("HTTP/1.0 200", 0));
    ASSERT_NE(std::string::npos, resp.find("X-GP-PROTO: 1"));
    std::string body = resp.substr(resp.find("\r\n\r\n") + 4);

    size_t consumed = 0;
    auto frames = parse_frames(body, &consumed);
    ASSERT_TRUE(frames.ok());
    std::string data;
    bool eof = false;
    for (const auto& f : *frames) {
        if (f.type == FrameType::DATA) {
            if (f.payload.empty()) {
                eof = true;
            } else {
                data += f.payload;
            }
        }
    }
    EXPECT_TRUE(eof);
    EXPECT_EQ("10\tred\n11\tblue\n", data);
    SessionRegistry::instance()->remove(token, 0);
}

// Unknown, non-tombstoned token: 404 for both verbs.
TEST_F(GpfdistServerTest, unknown_token_404) {
    EXPECT_EQ(404, post(port(), "no_such_token", 0, "X-GP-SEQ: 1\r\n", ""));
    int fd = connect_local(port());
    send_all(fd, "GET /no_such_token HTTP/1.0\r\n" + base_headers(0, 1) + "\r\n");
    std::string resp = recv_all(fd);
    ::close(fd);
    EXPECT_EQ(0u, resp.rfind("HTTP/1.0 404", 0));
}

} // namespace starrocks::connector::gpfdist
