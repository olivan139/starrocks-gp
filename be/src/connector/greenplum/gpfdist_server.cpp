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

#include "connector/greenplum/gpfdist_server.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <thread>
#include <vector>

#include "util/time.h"
#include "common/config.h"
#include "common/logging.h"
#include "connector/greenplum/gpfdist_protocol.h"
#include "connector/greenplum/gpfdist_session.h"
#include "fmt/format.h"

namespace starrocks::connector::gpfdist {

namespace {

constexpr size_t MAX_HEAD_BYTES = 64 * 1024;
// Mirrors gpfdist's write_file_buffer hard cap: segment send buffers are
// configured in KB..MB; 1GB is a protects-against-garbage bound, not a target.
constexpr int64_t MAX_BODY_BYTES = 1LL << 30;
constexpr int64_t SWEEP_INTERVAL_MS = 30 * 1000;
// How long a GET waits for the sink to produce the next block before giving
// up on the whole load (the pipeline normally produces continuously).
constexpr int64_t GET_BLOCK_TIMEOUT_MS = 10 * 60 * 1000;

struct ServerState {
    std::mutex sweep_mu;
    std::condition_variable sweep_cv;
    std::atomic<bool> started{false};
    Status start_status;
    int listen_fd = -1;
    int32_t port = 0;
    std::atomic<bool> stopping{false};
    std::thread accept_thread;
    std::thread sweep_thread;
    std::mutex once_mu;
};

ServerState* state() {
    static ServerState s;
    return &s;
}

bool send_all(int fd, std::string_view data) {
    size_t off = 0;
    while (off < data.size()) {
        ssize_t n = ::send(fd, data.data() + off, data.size() - off, 0);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

// Read until the buffer contains the full request head; returns bytes of the
// head, with any extra bytes (start of body) left in `buf` after it.
StatusOr<size_t> read_head(int fd, std::string* buf) {
    char tmp[8192];
    while (true) {
        if (auto end = find_head_end(*buf); end.has_value()) return *end;
        if (buf->size() > MAX_HEAD_BYTES) {
            return Status::InvalidArgument("request head too large");
        }
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) return Status::InvalidArgument("connection closed before request head completed");
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::InternalError(fmt::format("recv failed: {}", strerror(errno)));
        }
        buf->append(tmp, static_cast<size_t>(n));
    }
}

Status read_body(int fd, std::string* body, int64_t remaining) {
    char tmp[64 * 1024];
    while (remaining > 0) {
        ssize_t n = ::recv(fd, tmp, std::min<int64_t>(remaining, sizeof(tmp)), 0);
        if (n == 0) return Status::InvalidArgument("connection closed mid-body");
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::InternalError(fmt::format("recv failed: {}", strerror(errno)));
        }
        body->append(tmp, static_cast<size_t>(n));
        remaining -= n;
    }
    return Status::OK();
}

std::string token_of(const RequestHeaders& r) {
    std::string_view p = r.path;
    if (!p.empty() && p.front() == '/') p.remove_prefix(1);
    // strip any sub-path / query the FE never generates but a proxy might add
    size_t cut = p.find_first_of("/?");
    if (cut != std::string_view::npos) p = p.substr(0, cut);
    return std::string(p);
}

void handle_post(int fd, const RequestHeaders& r, std::string body) {
    auto* reg = SessionRegistry::instance();
    std::string token = token_of(r);
    reg->touch(token, MonotonicMillis());

    auto push = reg->find_push(token);
    if (push == nullptr) {
        if (reg->is_tombstoned(token)) {
            // Late X-GP-DONE for a completed session must succeed
            // (gpfdist.c:1900 "got a final write request...").
            send_all(fd, build_ok_response(r.gp_proto));
        } else {
            send_all(fd, build_error_response(404, fmt::format("unknown gpfdist session '{}'", token)));
        }
        return;
    }
    if (!r.is_done && push->over_capacity(static_cast<int64_t>(body.size()))) {
        // Segment backs off and retries the same seq (url_curl.c:558).
        send_all(fd, build_error_response(408, "buffer full, retry"));
        return;
    }
    auto action = push->on_post(r.segment_id, r.seq, r.is_done, std::move(body));
    if (!action.ok()) {
        send_all(fd, build_error_response(400, action.status().message()));
        return;
    }
    send_all(fd, build_ok_response(r.gp_proto));
}

void handle_get(int fd, const RequestHeaders& r) {
    auto* reg = SessionRegistry::instance();
    std::string token = token_of(r);
    reg->touch(token, MonotonicMillis());

    auto pull = reg->find_pull(token);
    if (pull == nullptr) {
        if (reg->is_tombstoned(token)) {
            // Completed load re-polled: empty stream (200 + immediate EOF).
            std::string out = build_ok_response(r.gp_proto);
            if (r.gp_proto == 1) append_eof_frame(&out);
            send_all(fd, out);
        } else {
            send_all(fd, build_error_response(404, fmt::format("unknown gpfdist session '{}'", token)));
        }
        return;
    }

    if (!send_all(fd, build_ok_response(r.gp_proto))) return;
    int64_t offset = 0;
    int64_t line_no = 0;
    while (true) {
        auto block = pull->next_block(GET_BLOCK_TIMEOUT_MS);
        reg->touch(token, MonotonicMillis());
        if (!block.ok()) {
            // Headers already sent: the in-band error channel is the E frame,
            // whose text surfaces to the Greenplum user (url_curl.c 'E' path).
            std::string out;
            if (r.gp_proto == 1) {
                append_frame(&out, FrameType::ERR, block.status().message());
                send_all(fd, out);
            }
            return; // close aborts the segment's scan for proto 0
        }
        if (!block->has_value()) { // clean EOF: sink finished, queue drained
            std::string out;
            if (r.gp_proto == 1) {
                append_eof_frame(&out);
                send_all(fd, out);
            }
            return;
        }
        std::string out;
        if (r.gp_proto == 1) {
            append_block_prelude(&out, "starrocks", offset, line_no);
            append_frame(&out, FrameType::DATA, **block);
        } else {
            out = std::move(**block);
        }
        offset += static_cast<int64_t>((*block)->size());
        if (!send_all(fd, out)) {
            // Segment went away mid-stream; its block is LOST for this load.
            // The FE row-count reconciliation catches it and fails the load -
            // never re-queue: another segment may already have later blocks.
            pull->fail(Status::InternalError("segment connection lost mid-stream"));
            return;
        }
    }
}

void serve_connection(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    std::string buf;
    auto head_len = read_head(fd, &buf);
    if (!head_len.ok()) {
        send_all(fd, build_error_response(400, head_len.status().message()));
        ::close(fd);
        return;
    }
    auto parsed = parse_request_head(std::string_view(buf).substr(0, *head_len));
    if (!parsed.ok()) {
        send_all(fd, build_error_response(400, parsed.status().message()));
        ::close(fd);
        return;
    }
    RequestHeaders& r = *parsed;

    if (r.method == Method::POST) {
        int64_t content_length = r.content_length < 0 ? 0 : r.content_length;
        if (content_length > MAX_BODY_BYTES) {
            send_all(fd, build_error_response(400, "invalid Content-Length"));
            ::close(fd);
            return;
        }
        if (r.wants_continue) {
            if (!send_all(fd, build_continue_response())) {
                ::close(fd);
                return;
            }
        }
        std::string body = buf.substr(*head_len); // bytes that arrived with the head
        if (static_cast<int64_t>(body.size()) < content_length) {
            if (Status st = read_body(fd, &body, content_length - body.size()); !st.ok()) {
                send_all(fd, build_error_response(400, st.message()));
                ::close(fd);
                return;
            }
        }
        body.resize(content_length);
        handle_post(fd, r, std::move(body));
    } else {
        handle_get(fd, r);
    }
    ::close(fd);
}

void accept_loop() {
    auto* s = state();
    while (!s->stopping.load()) {
        int fd = ::accept(s->listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (s->stopping.load()) break;
            LOG(WARNING) << "gpfdist accept failed: " << strerror(errno);
            continue;
        }
        std::thread(serve_connection, fd).detach();
    }
}

void sweep_loop() {
    auto* s = state();
    std::unique_lock<std::mutex> lk(s->sweep_mu);
    while (!s->stopping.load()) {
        s->sweep_cv.wait_for(lk, std::chrono::milliseconds(SWEEP_INTERVAL_MS));
        if (s->stopping.load()) break;
        SessionRegistry::instance()->sweep(MonotonicMillis(), config::greenplum_gpfdist_session_ttl_ms);
    }
}

} // namespace

Status GpfdistServer::start_once(int32_t port) {
    auto* s = state();
    std::lock_guard<std::mutex> lk(s->once_mu);
    if (s->started.load()) return s->start_status;

    if (config::greenplum_gpfdist_enable_tls) {
        s->start_status = Status::NotSupported(
                "gpfdists (TLS) is not implemented yet; unset greenplum_gpfdist_enable_tls");
        s->started.store(true);
        return s->start_status;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        s->start_status = Status::InternalError(fmt::format("socket() failed: {}", strerror(errno)));
        s->started.store(true);
        return s->start_status;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 || ::listen(fd, 128) < 0) {
        s->start_status = Status::InternalError(
                fmt::format("gpfdist bind/listen on port {} failed: {}", port, strerror(errno)));
        ::close(fd);
        s->started.store(true);
        return s->start_status;
    }
    socklen_t alen = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &alen);
    s->port = ntohs(addr.sin_port);
    s->listen_fd = fd;
    s->stopping.store(false);
    s->accept_thread = std::thread(accept_loop);
    s->sweep_thread = std::thread(sweep_loop);
    s->start_status = Status::OK();
    s->started.store(true);
    LOG(INFO) << "gpfdist server listening on port " << s->port;
    return s->start_status;
}

void GpfdistServer::shutdown() {
    auto* s = state();
    std::lock_guard<std::mutex> lk(s->once_mu);
    if (!s->started.load() || !s->start_status.ok()) return;
    s->stopping.store(true);
    s->sweep_cv.notify_all();
    ::shutdown(s->listen_fd, SHUT_RDWR);
    ::close(s->listen_fd);
    if (s->accept_thread.joinable()) s->accept_thread.join();
    if (s->sweep_thread.joinable()) s->sweep_thread.join(); // <= sweep interval wait
    s->started.store(false);
    s->port = 0;
}

int32_t GpfdistServer::bound_port() {
    return state()->port;
}

} // namespace starrocks::connector::gpfdist
