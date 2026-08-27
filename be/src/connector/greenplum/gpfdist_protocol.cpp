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

#include <arpa/inet.h>

#include <cctype>
#include <charconv>
#include <cstring>

#include "fmt/format.h"

namespace starrocks::connector::gpfdist {

namespace {

// case-insensitive ASCII compare (header names are case-insensitive per HTTP;
// gpfdist itself mixes strcmp/strcasecmp - we accept both spellings).
bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r')) s.remove_suffix(1);
    return s;
}

std::optional<int64_t> parse_int(std::string_view s) {
    int64_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || ptr != s.data() + s.size()) return std::nullopt;
    return v;
}

int64_t hton64(int64_t v) {
    uint64_t u = static_cast<uint64_t>(v);
    if constexpr (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
        u = (static_cast<uint64_t>(htonl(static_cast<uint32_t>(u))) << 32) | htonl(static_cast<uint32_t>(u >> 32));
    }
    return static_cast<int64_t>(u);
}

} // namespace

std::string RequestHeaders::session_key() const {
    return fmt::format("{}.{}.{}.{}:{}", xid, cid, sn, gp_proto, path);
}

std::optional<size_t> find_head_end(std::string_view buffer) {
    size_t pos = buffer.find("\r\n\r\n");
    if (pos == std::string_view::npos) return std::nullopt;
    return pos + 4;
}

StatusOr<std::string> base16_decode(std::string_view in) {
    if (in.size() % 2 != 0) {
        return Status::InvalidArgument("invalid base16 encoding: odd length");
    }
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string out;
    out.reserve(in.size() / 2);
    for (size_t i = 0; i < in.size(); i += 2) {
        int hi = nibble(in[i]);
        int lo = nibble(in[i + 1]);
        if (hi < 0 || lo < 0) {
            return Status::InvalidArgument("invalid base16 encoding: non-hex character");
        }
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return out;
}

StatusOr<RequestHeaders> parse_request_head(std::string_view head) {
    RequestHeaders r;

    size_t line_end = head.find("\r\n");
    if (line_end == std::string_view::npos) {
        return Status::InvalidArgument("malformed request: no request line");
    }
    std::string_view request_line = head.substr(0, line_end);

    // "<METHOD> <path> HTTP/1.x"
    size_t sp1 = request_line.find(' ');
    size_t sp2 = request_line.rfind(' ');
    if (sp1 == std::string_view::npos || sp2 == sp1) {
        return Status::InvalidArgument(fmt::format("malformed request line: '{}'", request_line));
    }
    std::string_view method = request_line.substr(0, sp1);
    r.path = std::string(trim(request_line.substr(sp1 + 1, sp2 - sp1 - 1)));
    if (method == "GET") {
        r.method = Method::GET;
    } else if (method == "POST") {
        r.method = Method::POST;
    } else {
        return Status::InvalidArgument(fmt::format("unsupported method '{}'", method));
    }

    bool has_xid = false;
    bool has_cid = false;
    bool has_sn = false;
    std::string line_delim_b16;
    int64_t line_delim_len = -1;

    size_t pos = line_end + 2;
    while (pos < head.size()) {
        size_t eol = head.find("\r\n", pos);
        if (eol == std::string_view::npos || eol == pos) break; // blank line = end of headers
        std::string_view line = head.substr(pos, eol - pos);
        pos = eol + 2;

        size_t colon = line.find(':');
        if (colon == std::string_view::npos) continue;
        std::string_view name = trim(line.substr(0, colon));
        std::string_view value = trim(line.substr(colon + 1));

        if (iequals(name, "X-GP-XID")) {
            r.xid = std::string(value);
            has_xid = true;
        } else if (iequals(name, "X-GP-CID")) {
            r.cid = std::string(value);
            has_cid = true;
        } else if (iequals(name, "X-GP-SN")) {
            r.sn = std::string(value);
            has_sn = true;
        } else if (iequals(name, "X-GP-PROTO")) {
            auto v = parse_int(value);
            if (!v) return Status::InvalidArgument("invalid X-GP-PROTO");
            r.gp_proto = static_cast<int>(*v);
        } else if (iequals(name, "X-GP-SEGMENT-ID")) {
            if (auto v = parse_int(value)) r.segment_id = static_cast<int>(*v);
        } else if (iequals(name, "X-GP-SEGMENT-COUNT")) {
            if (auto v = parse_int(value)) r.segment_count = static_cast<int>(*v);
        } else if (iequals(name, "X-GP-SEQ")) {
            auto v = parse_int(value);
            // sequence numbers start at 1 (gpfdist.c:3919 rejects seq <= 0)
            if (!v || *v <= 0) return Status::InvalidArgument("invalid X-GP-SEQ");
            r.seq = *v;
        } else if (iequals(name, "X-GP-DONE")) {
            r.is_done = true;
        } else if (iequals(name, "X-GP-CSVOPT")) {
            r.csvopt = std::string(value);
        } else if (iequals(name, "X-GP-ZSTD")) {
            r.zstd_requested = true; // we never enable it in responses
        } else if (iequals(name, "X-GP-LINE-DELIM-STR")) {
            line_delim_b16 = std::string(value);
        } else if (iequals(name, "X-GP-LINE-DELIM-LENGTH")) {
            if (auto v = parse_int(value)) line_delim_len = *v;
        } else if (iequals(name, "X-GP-TRANSFORM")) {
            return Status::NotSupported("gpfxdist transforms are not supported");
        } else if (iequals(name, "Expect")) {
            if (iequals(value, "100-continue")) r.wants_continue = true;
        } else if (iequals(name, "Content-Length")) {
            auto v = parse_int(value);
            if (!v || *v < 0) return Status::InvalidArgument("invalid Content-Length");
            r.content_length = *v;
        }
    }

    // X-GP-PROTO is mandatory and must be 0 or 1 (gpfdist.c:3966-3990).
    if (r.gp_proto != 0 && r.gp_proto != 1) {
        return Status::InvalidArgument("invalid request (no or bad X-GP-PROTO)");
    }
    // XID/CID/SN: all or none (gpfdist.c:3995-4003).
    if ((has_xid || has_cid || has_sn) && !(has_xid && has_cid && has_sn)) {
        return Status::InvalidArgument("invalid request (missing X-GP-* header)");
    }
    if (!line_delim_b16.empty()) {
        ASSIGN_OR_RETURN(r.line_delim, base16_decode(line_delim_b16));
        if (line_delim_len >= 0 && static_cast<int64_t>(r.line_delim.size()) != line_delim_len) {
            return Status::InvalidArgument("invalid EOL length");
        }
    }
    return r;
}

std::string build_ok_response(int gp_proto) {
    // Field-for-field mirror of gpfdist.c:474 HTTP_RESPONSE. HTTP/1.0 +
    // "Connection: close": the body (if any) is terminated by connection close.
    return fmt::format(
            "HTTP/1.0 200 ok\r\n"
            "Content-type: text/plain\r\n"
            "Expires: 0\r\n"
            "X-GPFDIST-VERSION: starrocks-greenplum-connector\r\n"
            "X-GP-PROTO: {}\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: close\r\n\r\n",
            gp_proto);
}

std::string build_continue_response() {
    return "HTTP/1.0 100 Continue\r\n\r\n";
}

std::string build_error_response(int http_status, std::string_view reason) {
    // The reason phrase is what the GP user sees (url_curl.c keeps the whole
    // status line and reports it in check_response) - keep it single-line.
    std::string clean(reason);
    for (char& c : clean) {
        if (c == '\r' || c == '\n') c = ' ';
    }
    return fmt::format(
            "HTTP/1.0 {} {}\r\n"
            "Content-type: text/plain\r\n"
            "Connection: close\r\n\r\n",
            http_status, clean);
}

void append_frame(std::string* out, FrameType type, std::string_view payload) {
    out->push_back(static_cast<char>(type));
    uint32_t len = htonl(static_cast<uint32_t>(payload.size()));
    out->append(reinterpret_cast<const char*>(&len), 4);
    out->append(payload.data(), payload.size());
}

void append_block_prelude(std::string* out, std::string_view name, int64_t offset, int64_t line_number) {
    append_frame(out, FrameType::FILENAME, name);
    int64_t off_n = hton64(offset);
    append_frame(out, FrameType::OFFSET, std::string_view(reinterpret_cast<const char*>(&off_n), 8));
    int64_t line_n = hton64(line_number);
    append_frame(out, FrameType::LINENO, std::string_view(reinterpret_cast<const char*>(&line_n), 8));
}

void append_eof_frame(std::string* out) {
    append_frame(out, FrameType::DATA, {});
}

StatusOr<std::vector<Frame>> parse_frames(std::string_view buffer, size_t* consumed) {
    std::vector<Frame> frames;
    size_t pos = 0;
    while (buffer.size() - pos >= 5) {
        char t = buffer[pos];
        if (t != 'F' && t != 'O' && t != 'L' && t != 'D' && t != 'E') {
            return Status::Corruption(fmt::format("unknown gpfdist frame type {}", static_cast<int>(t)));
        }
        uint32_t len_n;
        memcpy(&len_n, buffer.data() + pos + 1, 4);
        uint32_t len = ntohl(len_n);
        if (buffer.size() - pos - 5 < len) break; // partial frame
        frames.push_back(Frame{static_cast<FrameType>(t), std::string(buffer.substr(pos + 5, len))});
        pos += 5 + len;
    }
    *consumed = pos;
    return frames;
}

} // namespace starrocks::connector::gpfdist
