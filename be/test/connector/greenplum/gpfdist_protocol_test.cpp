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

#include "connector/greenplum/gpfdist_protocol.h"

#include <gtest/gtest.h>

namespace starrocks::connector::gpfdist {

static std::string post_head(const std::string& extra) {
    return "POST /tok_abc HTTP/1.0\r\n"
           "X-GP-XID: 1234\r\nX-GP-CID: 5\r\nX-GP-SN: 0\r\nX-GP-PROTO: 0\r\n"
           "X-GP-SEGMENT-ID: 3\r\nX-GP-SEGMENT-COUNT: 8\r\n" +
           extra + "\r\n";
}

TEST(GpfdistProtocolTest, parse_post_head) {
    auto r = parse_request_head(post_head("X-GP-SEQ: 2\r\nContent-Length: 100\r\nExpect: 100-continue\r\n"));
    ASSERT_TRUE(r.ok()) << r.status();
    EXPECT_EQ(Method::POST, r->method);
    EXPECT_EQ("/tok_abc", r->path);
    EXPECT_EQ(3, r->segment_id);
    EXPECT_EQ(8, r->segment_count);
    EXPECT_EQ(2, r->seq);
    EXPECT_EQ(100, r->content_length);
    EXPECT_TRUE(r->wants_continue);
    EXPECT_FALSE(r->is_done);
    EXPECT_EQ("1234.5.0.0:/tok_abc", r->session_key());
}

TEST(GpfdistProtocolTest, mandatory_proto_and_xid_triple) {
    EXPECT_FALSE(parse_request_head("GET /t HTTP/1.0\r\n\r\n").ok()); // no proto
    // partial XID triple must be rejected (gpfdist.c:3995)
    EXPECT_FALSE(parse_request_head("GET /t HTTP/1.0\r\nX-GP-PROTO: 1\r\nX-GP-XID: 1\r\n\r\n").ok());
    // full triple ok
    EXPECT_TRUE(parse_request_head("GET /t HTTP/1.0\r\nX-GP-PROTO: 1\r\n"
                                   "X-GP-XID: 1\r\nX-GP-CID: 2\r\nX-GP-SN: 3\r\n\r\n")
                        .ok());
    // proto other than 0/1 rejected
    EXPECT_FALSE(parse_request_head("GET /t HTTP/1.0\r\nX-GP-PROTO: 2\r\n\r\n").ok());
}

TEST(GpfdistProtocolTest, seq_and_done_and_transform) {
    EXPECT_FALSE(parse_request_head(post_head("X-GP-SEQ: 0\r\n")).ok());  // seq starts at 1
    EXPECT_FALSE(parse_request_head(post_head("X-GP-SEQ: -4\r\n")).ok());
    auto done = parse_request_head(post_head("X-GP-DONE: 1\r\n"));
    ASSERT_TRUE(done.ok());
    EXPECT_TRUE(done->is_done);
    // gpfxdist transforms are out of scope: loud rejection
    EXPECT_FALSE(parse_request_head(post_head("X-GP-TRANSFORM: sed\r\n")).ok());
}

TEST(GpfdistProtocolTest, line_delim_base16) {
    auto r = parse_request_head(post_head("X-GP-LINE-DELIM-STR: 0a\r\nX-GP-LINE-DELIM-LENGTH: 1\r\n"));
    ASSERT_TRUE(r.ok()) << r.status();
    EXPECT_EQ("\n", r->line_delim);
    // odd length rejected, mirroring gpfdist.c:3897
    EXPECT_FALSE(parse_request_head(post_head("X-GP-LINE-DELIM-STR: 0a0\r\n")).ok());
    // length mismatch rejected
    EXPECT_FALSE(parse_request_head(post_head("X-GP-LINE-DELIM-STR: 0a\r\nX-GP-LINE-DELIM-LENGTH: 2\r\n")).ok());
}

TEST(GpfdistProtocolTest, find_head_end) {
    EXPECT_FALSE(find_head_end("GET / HTTP/1.0\r\nX:").has_value());
    auto head = post_head("");
    auto full = head + "BODYBYTES";
    auto end = find_head_end(full);
    ASSERT_TRUE(end.has_value());
    EXPECT_EQ(head.size(), *end);
}

TEST(GpfdistProtocolTest, responses) {
    auto ok = build_ok_response(1);
    EXPECT_NE(std::string::npos, ok.find("HTTP/1.0 200 ok\r\n"));
    EXPECT_NE(std::string::npos, ok.find("X-GP-PROTO: 1\r\n"));
    EXPECT_NE(std::string::npos, ok.find("Connection: close\r\n"));
    EXPECT_EQ(std::string::npos, ok.find("ZSTD")); // we always decline compression

    EXPECT_EQ("HTTP/1.0 100 Continue\r\n\r\n", build_continue_response());

    // error text reaches the GP user via the status line - newlines must not break framing
    auto err = build_error_response(500, "boom\r\nmore");
    EXPECT_NE(std::string::npos, err.find("HTTP/1.0 500 boom  more\r\n"));
}

TEST(GpfdistProtocolTest, frames_roundtrip) {
    std::string out;
    append_block_prelude(&out, "starrocks", 0, 1);
    append_frame(&out, FrameType::DATA, "a\tb\n");
    append_eof_frame(&out);

    size_t consumed = 0;
    auto frames = parse_frames(out, &consumed);
    ASSERT_TRUE(frames.ok()) << frames.status();
    EXPECT_EQ(out.size(), consumed);
    ASSERT_EQ(5u, frames->size());
    EXPECT_EQ(FrameType::FILENAME, (*frames)[0].type);
    EXPECT_EQ("starrocks", (*frames)[0].payload);
    EXPECT_EQ(FrameType::OFFSET, (*frames)[1].type);
    EXPECT_EQ(8u, (*frames)[1].payload.size());
    EXPECT_EQ(FrameType::LINENO, (*frames)[2].type);
    EXPECT_EQ(FrameType::DATA, (*frames)[3].type);
    EXPECT_EQ("a\tb\n", (*frames)[3].payload);
    EXPECT_EQ(FrameType::DATA, (*frames)[4].type);
    EXPECT_TRUE((*frames)[4].payload.empty()); // EOF marker: D with len 0
}

TEST(GpfdistProtocolTest, frames_partial_and_garbage) {
    std::string out;
    append_frame(&out, FrameType::DATA, "hello");
    size_t consumed = 0;
    // cut mid-frame: nothing consumed, no error
    auto partial = parse_frames(std::string_view(out).substr(0, 7), &consumed);
    ASSERT_TRUE(partial.ok());
    EXPECT_TRUE(partial->empty());
    EXPECT_EQ(0u, consumed);
    // unknown frame type is corruption
    EXPECT_FALSE(parse_frames("Xaaaaaaa", &consumed).ok());
}

TEST(GpfdistProtocolTest, base16) {
    auto v = base16_decode("09");
    ASSERT_TRUE(v.ok());
    EXPECT_EQ("\t", *v);
    EXPECT_FALSE(base16_decode("0").ok());
    EXPECT_FALSE(base16_decode("zz").ok());
}

} // namespace starrocks::connector::gpfdist
