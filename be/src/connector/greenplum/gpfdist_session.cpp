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

#include "connector/greenplum/gpfdist_session.h"

#include <chrono>
#include <iterator>

#include "fmt/format.h"

namespace starrocks::connector::gpfdist {

namespace {
constexpr int64_t OPEN_SEQ = 1; // gpfdist.c: "sequence number is 1, it's the first OPEN request"
}

// ---------------------------------------------------------------------------
// PushSession
// ---------------------------------------------------------------------------

PushSession::PushSession(std::string token, int64_t max_buffered_bytes)
        : _token(std::move(token)), _max_buffered_bytes(max_buffered_bytes) {}

StatusOr<PostAction> PushSession::on_post(int segment_id, int64_t seq, bool is_done, std::string body) {
    std::unique_lock<std::mutex> lk(_mu);
    if (!_failure.ok()) {
        return _failure;
    }
    if (segment_id < 0) {
        return Status::InvalidArgument("missing X-GP-SEGMENT-ID");
    }

    if (is_done) {
        // The final POST is empty and carries X-GP-DONE (url_curl.c:2009
        // gp_proto0_write_done). A DONE for a segment we never saw is legal:
        // that segment simply had nothing to send.
        _segment_done[segment_id] = true;
        _last_seq_per_segment.try_emplace(segment_id, 0);
        lk.unlock();
        _cv.notify_all();
        return PostAction::ACK_DONE;
    }

    int64_t& last = _last_seq_per_segment[segment_id];
    if (seq == OPEN_SEQ && last == 0) {
        // First contact: open probe, empty body, nothing to consume.
        last = OPEN_SEQ;
        return PostAction::ACK_OPEN;
    }
    if (seq == last) {
        // The client RESENDS the same seq after a network error
        // (gp_perform_backoff_and_check_response). Ack, DISCARD the body:
        // appending it again would duplicate data - the whole point of seqs.
        return PostAction::ACK_DUPLICATE;
    }
    if (seq != last + 1) {
        // Out-of-order: the reference server 400s and the statement aborts
        // (gpfdist.c:3540-3555). Never try to reorder.
        return Status::InvalidArgument(fmt::format(
                "out of order gpfdist request from segment {}: got seq {}, expected {}", segment_id, seq, last + 1));
    }
    last = seq;
    if (!body.empty()) {
        _buffered_bytes += static_cast<int64_t>(body.size());
        _total_bytes += static_cast<int64_t>(body.size());
        _buffered.push_back(std::move(body));
        lk.unlock();
        _cv.notify_all();
    }
    return PostAction::ACCEPT;
}

bool PushSession::all_segments_done() const {
    std::lock_guard<std::mutex> lk(_mu);
    if (_segment_done.empty()) return false;
    for (const auto& [seg, last] : _last_seq_per_segment) {
        auto it = _segment_done.find(seg);
        if (it == _segment_done.end() || !it->second) return false;
    }
    return true;
}

StatusOr<std::optional<std::string>> PushSession::take(int64_t timeout_ms) {
    std::unique_lock<std::mutex> lk(_mu);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        if (!_failure.ok()) return _failure;
        if (!_buffered.empty()) {
            std::string slab = std::move(_buffered.front());
            _buffered.pop_front();
            _buffered_bytes -= static_cast<int64_t>(slab.size());
            return std::optional<std::string>(std::move(slab));
        }
        // EOF only when every attached segment said DONE and we drained.
        bool done = !_segment_done.empty();
        for (const auto& [seg, last] : _last_seq_per_segment) {
            auto it = _segment_done.find(seg);
            if (it == _segment_done.end() || !it->second) {
                done = false;
                break;
            }
        }
        if (done) return std::optional<std::string>();
        if (_cv.wait_until(lk, deadline) == std::cv_status::timeout) {
            return Status::TimedOut(fmt::format("gpfdist session {}: no data within {} ms", _token, timeout_ms));
        }
    }
}

bool PushSession::over_capacity(int64_t incoming_bytes) const {
    std::lock_guard<std::mutex> lk(_mu);
    return _buffered_bytes + incoming_bytes > _max_buffered_bytes;
}

void PushSession::fail(Status reason) {
    {
        std::lock_guard<std::mutex> lk(_mu);
        if (_failure.ok()) _failure = std::move(reason);
    }
    _cv.notify_all();
}

int64_t PushSession::total_rows_hint() const {
    std::lock_guard<std::mutex> lk(_mu);
    return _total_bytes;
}

int64_t PushSession::total_bytes() const {
    std::lock_guard<std::mutex> lk(_mu);
    return _total_bytes;
}

// ---------------------------------------------------------------------------
// PullSession
// ---------------------------------------------------------------------------

PullSession::PullSession(std::string token, int64_t max_buffered_bytes)
        : _token(std::move(token)), _max_buffered_bytes(max_buffered_bytes) {}

Status PullSession::put_block(std::string block, int64_t timeout_ms) {
    std::unique_lock<std::mutex> lk(_mu);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        if (!_failure.ok()) return _failure;
        if (_finished) return Status::InternalError("put_block after finish()");
        if (_buffered_bytes + static_cast<int64_t>(block.size()) <= _max_buffered_bytes || _blocks.empty()) {
            _buffered_bytes += static_cast<int64_t>(block.size());
            _blocks.push_back(std::move(block));
            lk.unlock();
            _cv_take.notify_one();
            return Status::OK();
        }
        if (_cv_put.wait_until(lk, deadline) == std::cv_status::timeout) {
            return Status::TimedOut(
                    fmt::format("gpfdist session {}: segments not pulling, backpressure timeout {} ms", _token,
                                timeout_ms));
        }
    }
}

void PullSession::finish() {
    {
        std::lock_guard<std::mutex> lk(_mu);
        _finished = true;
    }
    _cv_take.notify_all();
}

StatusOr<std::optional<std::string>> PullSession::next_block(int64_t timeout_ms) {
    std::unique_lock<std::mutex> lk(_mu);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        if (!_failure.ok()) return _failure;
        if (!_blocks.empty()) {
            std::string block = std::move(_blocks.front());
            _blocks.pop_front();
            _buffered_bytes -= static_cast<int64_t>(block.size());
            lk.unlock();
            _cv_put.notify_one();
            return std::optional<std::string>(std::move(block));
        }
        if (_finished) return std::optional<std::string>(); // clean EOF
        if (_cv_take.wait_until(lk, deadline) == std::cv_status::timeout) {
            // Caller answers HTTP 408: the segment retries with backoff.
            return Status::TimedOut(fmt::format("gpfdist session {}: no block ready", _token));
        }
    }
}

void PullSession::fail(Status reason) {
    {
        std::lock_guard<std::mutex> lk(_mu);
        if (_failure.ok()) _failure = std::move(reason);
    }
    _cv_take.notify_all();
    _cv_put.notify_all();
}

// ---------------------------------------------------------------------------
// SessionRegistry
// ---------------------------------------------------------------------------

SessionRegistry* SessionRegistry::instance() {
    static SessionRegistry registry;
    return &registry;
}

std::shared_ptr<PushSession> SessionRegistry::create_push(const std::string& token, int64_t max_buffered_bytes) {
    std::lock_guard<std::mutex> lk(_mu);
    auto& e = _sessions[token];
    if (e.push == nullptr) e.push = std::make_shared<PushSession>(token, max_buffered_bytes);
    return e.push;
}

std::shared_ptr<PullSession> SessionRegistry::create_pull(const std::string& token, int64_t max_buffered_bytes) {
    std::lock_guard<std::mutex> lk(_mu);
    auto& e = _sessions[token];
    if (e.pull == nullptr) e.pull = std::make_shared<PullSession>(token, max_buffered_bytes);
    return e.pull;
}

std::shared_ptr<PushSession> SessionRegistry::find_push(const std::string& token) {
    std::lock_guard<std::mutex> lk(_mu);
    auto it = _sessions.find(token);
    return it == _sessions.end() ? nullptr : it->second.push;
}

std::shared_ptr<PullSession> SessionRegistry::find_pull(const std::string& token) {
    std::lock_guard<std::mutex> lk(_mu);
    auto it = _sessions.find(token);
    return it == _sessions.end() ? nullptr : it->second.pull;
}

bool SessionRegistry::is_tombstoned(const std::string& token) {
    std::lock_guard<std::mutex> lk(_mu);
    return _tombstones.count(token) > 0;
}

void SessionRegistry::touch(const std::string& token, int64_t now_ms) {
    std::lock_guard<std::mutex> lk(_mu);
    auto it = _sessions.find(token);
    if (it != _sessions.end()) it->second.last_touch_ms = now_ms;
}

void SessionRegistry::remove(const std::string& token, int64_t now_ms) {
    std::lock_guard<std::mutex> lk(_mu);
    _sessions.erase(token);
    _tombstones[token] = now_ms;
}

void SessionRegistry::sweep(int64_t now_ms, int64_t ttl_ms) {
    std::lock_guard<std::mutex> lk(_mu);
    for (auto it = _tombstones.begin(); it != _tombstones.end();) {
        it = (now_ms - it->second > ttl_ms) ? _tombstones.erase(it) : std::next(it);
    }
    for (auto it = _sessions.begin(); it != _sessions.end();) {
        if (now_ms - it->second.last_touch_ms > ttl_ms && it->second.last_touch_ms > 0) {
            Status reason = Status::Aborted("gpfdist session expired (owner gone?)");
            if (it->second.push) it->second.push->fail(reason);
            if (it->second.pull) it->second.pull->fail(reason);
            _tombstones[it->first] = now_ms;
            it = _sessions.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace starrocks::connector::gpfdist
