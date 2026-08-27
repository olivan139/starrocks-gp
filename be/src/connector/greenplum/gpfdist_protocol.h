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

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"

// Pure functions implementing the gpfdist wire protocol. NO I/O in this file:
// everything here is a (bytes in -> struct out) or (struct in -> bytes out)
// transformation, unit-testable without sockets.
//
// The authoritative contract is the segment-side client in the ADB sources:
//   gpdb/src/backend/access/external/url_curl.c   (what segments send/accept)
//   gpdb/src/bin/gpfdist/gpfdist.c                (the reference server)
// Distilled spec with line references: ADB_GPFDIST_PROTOCOL_NOTES.md (repo root).
//
// Wire basics: HTTP/1.0, "Connection: close", no chunked encoding. GET
// responses stream PROTO-1 frames until close; POST requests carry
// Content-Length bodies and may ask for "100-continue".

namespace starrocks::connector::gpfdist {

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

enum class Method { GET, POST, UNSUPPORTED };

// Parsed X-GP-* headers of one request (url_curl.c:1332-1400 sets these;
// gpfdist.c:3860 request_parse_gp_headers is the reference parser).
struct RequestHeaders {
    Method method = Method::UNSUPPORTED;
    std::string path; // URI path, e.g. "/<session_token>"

    std::string xid; // X-GP-XID   distributed txn id
    std::string cid; // X-GP-CID   command id
    std::string sn;  // X-GP-SN    scan number
    int gp_proto = -1;        // X-GP-PROTO; mandatory, 0 or 1; reject others
    int segment_id = -1;      // X-GP-SEGMENT-ID
    int segment_count = -1;   // X-GP-SEGMENT-COUNT
    int64_t seq = 0;          // X-GP-SEQ; write path only; starts at 1
    bool is_done = false;     // X-GP-DONE present
    bool wants_continue = false;   // "Expect: 100-continue"
    int64_t content_length = -1;   // Content-Length, if present
    std::string line_delim;        // X-GP-LINE-DELIM-STR, base16-DECODED
    std::string csvopt;            // X-GP-CSVOPT (readable-table GETs only)
    bool zstd_requested = false;   // ADB extension; we decline (protocol-legal)

    // Session identity: all requests of one GP statement share it
    // (gpfdist.c:1864 session_attach builds "xid.cid.sn.proto:path").
    std::string session_key() const;
};

// Parse the full request head: request line + headers, up to and including the
// terminating CRLFCRLF. `head` must NOT include any body bytes.
// Fails (InvalidArgument) on: malformed request line, method other than
// GET/POST, missing/invalid X-GP-PROTO, partial XID/CID/SN triple, odd-length
// X-GP-LINE-DELIM-STR (mirrors gpfdist.c:3897 rejection), seq <= 0 when
// X-GP-SEQ present.
StatusOr<RequestHeaders> parse_request_head(std::string_view head);

// Locate the end of the request head in a receive buffer: returns the offset
// one-past the CRLFCRLF terminator, or nullopt if the head is incomplete.
std::optional<size_t> find_head_end(std::string_view buffer);

// base16 ("hex") decode as used by X-GP-LINE-DELIM-STR. Fails on odd length
// or non-hex characters.
StatusOr<std::string> base16_decode(std::string_view in);

// ---------------------------------------------------------------------------
// Responses (HTTP/1.0, hand-rolled: gpfdist.c:474 HTTP_RESPONSE)
// ---------------------------------------------------------------------------

// 200 header block for a GET/POST. The body (frames) is streamed after it for
// GETs; POST responses end after the header block. Always announces
// "X-GP-PROTO: <proto>" and "Connection: close". Never announces zstd.
std::string build_ok_response(int gp_proto);

// "HTTP/1.0 100 Continue\r\n\r\n" - required before reading a POST body when
// the client sent "Expect: 100-continue" (gpfdist.c:3489).
std::string build_continue_response();

// Error response. `reason` lands in the status line: url_curl.c check_response
// surfaces it verbatim to the Greenplum user, so make it actionable.
// Status 408 is special: the client retries with backoff instead of failing
// (url_curl.c:558) - use it for backpressure, not for errors.
std::string build_error_response(int http_status, std::string_view reason);

// ---------------------------------------------------------------------------
// PROTO-1 frames: type(1B) | length(4B network order) | payload
// (url_curl.c:1694 gp_proto1_read; gpfdist.c block_fill_header)
// ---------------------------------------------------------------------------

enum class FrameType : char {
    FILENAME = 'F',
    OFFSET = 'O',
    LINENO = 'L',
    DATA = 'D',
    ERR = 'E',
};

// Append one frame to `out`.
void append_frame(std::string* out, FrameType type, std::string_view payload);

// The standard per-block metadata prelude the reference server sends before
// data: F(name) + O(offset,8B) + L(lineno,8B). Offsets/linenos are
// network-order int64 (gpfdist.c block_fill_header).
void append_block_prelude(std::string* out, std::string_view name, int64_t offset, int64_t line_number);

// EOF marker: DATA frame with length 0 (url_curl.c:1755 "eof = (len == 0)").
void append_eof_frame(std::string* out);

// Parsed view of one frame during tests / future proxying.
struct Frame {
    FrameType type;
    std::string payload;
};

// Parse frames from a buffer; returns frames parsed and sets `consumed` to the
// number of bytes used (a trailing partial frame is left unconsumed). Fails on
// unknown frame type or negative length.
StatusOr<std::vector<Frame>> parse_frames(std::string_view buffer, size_t* consumed);

} // namespace starrocks::connector::gpfdist
