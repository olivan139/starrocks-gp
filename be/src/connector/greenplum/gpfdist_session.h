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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common/status.h"
#include "common/statusor.h"

// The gpfdist session registry: pure data structures + the protocol's
// correctness rules, deliberately free of any socket/libevent code so every
// rule is unit-testable in microseconds.
//
// A *session* is one external-table interaction of one Greenplum statement
// with THIS backend. All segments of the statement attach to the same session
// (they send the same URL path = our session token).
//
// Two directions:
//  - WRITE-TO-US (Greenplum pushes; our READ path, "INSERT INTO sr SELECT
//    FROM greenplum..."): segments send a series of POSTs; we assemble bodies
//    in per-segment sequence order and hand contiguous data to the scan.
//  - READ-FROM-US (Greenplum pulls; our WRITE path, "INSERT INTO greenplum
//    ... SELECT"): the BE sink enqueues encoded row-blocks; concurrent segment
//    GETs each take the NEXT block (one block -> exactly one segment).
//
// The POST rules below are a faithful port of gpfdist.c handle_post_request
// (l.3505-3560); getting them wrong silently duplicates or loses data.

namespace starrocks::connector::gpfdist {

// Outcome of offering one POST to the session (see PushSession::on_post).
enum class PostAction {
    ACK_OPEN,      // seq==1 open probe: reply 200, no data to consume
    ACCEPT,        // in-order data POST: body consumed, reply 200
    ACK_DUPLICATE, // retry of the last seq: reply 200, body DISCARDED
    ACK_DONE,      // X-GP-DONE processed (possibly for a finished session)
};

// ---------------------------------------------------------------------------
// WRITE-TO-US: per-session assembler feeding the scan-side DataSource.
// ---------------------------------------------------------------------------
class PushSession {
public:
    PushSession(std::string token, int64_t max_buffered_bytes);

    // Apply one POST request. `segment_id` >= 0; `seq` semantics:
    //   seq == 1              -> OPEN probe (empty body, just attach)
    //   seq == last_seen      -> duplicate after a client retry: ACK + drop
    //   seq == last_seen + 1  -> accept, append body
    //   anything else         -> InvalidArgument (client aborts the statement)
    // `is_done` handles the final empty POST with X-GP-DONE.
    // Returns the action taken, or an error the server must convert to an
    // HTTP 400/500 response.
    StatusOr<PostAction> on_post(int segment_id, int64_t seq, bool is_done, std::string body);

    // True once every segment that ever attached has sent DONE.
    bool all_segments_done() const;

    // Consumer side (DataSource::get_next): blocks until data, completion or
    // failure. Returns a data slab, or empty optional on clean EOF (all
    // segments done and buffer drained). Errors surface the failure set by
    // fail().
    StatusOr<std::optional<std::string>> take(int64_t timeout_ms);

    // Would accepting `bytes` more exceed the buffer bound? The server uses
    // this to answer 408 (client retries with backoff - url_curl.c:558)
    // instead of buffering without limit.
    bool over_capacity(int64_t incoming_bytes) const;

    // Terminal failure (query cancelled, decode error...): wakes the consumer
    // and makes further POSTs fail. Idempotent.
    void fail(Status reason);

    int64_t total_rows_hint() const; // bytes-based; real row count comes from the codec
    int64_t total_bytes() const;
    const std::string& token() const { return _token; }

private:
    mutable std::mutex _mu;
    std::condition_variable _cv;
    const std::string _token;
    const int64_t _max_buffered_bytes;

    std::map<int, int64_t> _last_seq_per_segment; // segment id -> last accepted seq
    std::map<int, bool> _segment_done;
    std::deque<std::string> _buffered; // accepted bodies, arrival order
    int64_t _buffered_bytes = 0;
    int64_t _total_bytes = 0;
    Status _failure = Status::OK();
};

// ---------------------------------------------------------------------------
// READ-FROM-US: per-session block queue fed by the ChunkSink.
// ---------------------------------------------------------------------------
class PullSession {
public:
    PullSession(std::string token, int64_t max_buffered_bytes);

    // Producer (GreenplumChunkSink::add): blocks while the queue is over
    // capacity (natural backpressure into the pipeline). Blocks must contain
    // WHOLE rows: segments parse their blocks independently.
    Status put_block(std::string block, int64_t timeout_ms);

    // Producer signals no more blocks (sink finished).
    void finish();

    // Consumer (a segment GET): next block to serve, empty optional = EOF
    // (finished and drained). One block goes to exactly ONE caller - this is
    // the whole work-distribution model (gpfdist.c session_get_block).
    StatusOr<std::optional<std::string>> next_block(int64_t timeout_ms);

    void fail(Status reason);
    int64_t rows_served() const { return _rows_served; }
    void add_rows_served(int64_t n) { _rows_served += n; }
    const std::string& token() const { return _token; }

private:
    mutable std::mutex _mu;
    std::condition_variable _cv_put;
    std::condition_variable _cv_take;
    const std::string _token;
    const int64_t _max_buffered_bytes;

    std::deque<std::string> _blocks;
    int64_t _buffered_bytes = 0;
    bool _finished = false;
    Status _failure = Status::OK();
    std::atomic<int64_t> _rows_served{0};
};

// ---------------------------------------------------------------------------
// Registry: token -> session, with tombstones and TTL sweep.
// ---------------------------------------------------------------------------
// Lifecycle notes (from the protocol, see notes doc):
//  - requests may arrive BEFORE the local query fragment opened the session
//    (segments connect at table-open time): the server answers 404 and the
//    segment fails the statement - the FE pre-registers by opening fragments
//    before issuing the GP statement, so this is a true error, not a race;
//  - an X-GP-DONE may arrive for an already-completed session (another
//    segment finished it): must be ACKed with 200, hence tombstones;
//  - sessions whose owner died must eventually disappear: remove() is the
//    explicit path, sweep() the TTL backstop (gpfdist "-k" analog).
class SessionRegistry {
public:
    static SessionRegistry* instance();

    std::shared_ptr<PushSession> create_push(const std::string& token, int64_t max_buffered_bytes);
    std::shared_ptr<PullSession> create_pull(const std::string& token, int64_t max_buffered_bytes);

    std::shared_ptr<PushSession> find_push(const std::string& token);
    std::shared_ptr<PullSession> find_pull(const std::string& token);

    // True if `token` was recently removed (completed): late DONEs get 200.
    bool is_tombstoned(const std::string& token);

    // Refresh a session's idle clock; the server calls this on every request
    // it routes to the session, so sweep() only reaps abandoned ones.
    void touch(const std::string& token, int64_t now_ms);

    // Explicit close by the owning fragment; leaves a tombstone.
    void remove(const std::string& token, int64_t now_ms);

    // Drop tombstones older than ttl and fail+drop sessions idle longer than
    // ttl (owner died without remove()). Call periodically from the server.
    void sweep(int64_t now_ms, int64_t ttl_ms);

private:
    struct Entry {
        std::shared_ptr<PushSession> push;
        std::shared_ptr<PullSession> pull;
        int64_t last_touch_ms = 0;
    };
    std::mutex _mu;
    std::map<std::string, Entry> _sessions;
    std::map<std::string, int64_t> _tombstones; // token -> removal time
};

} // namespace starrocks::connector::gpfdist
