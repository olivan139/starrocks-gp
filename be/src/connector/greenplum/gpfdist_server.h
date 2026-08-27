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

#include "common/status.h"

// The gpfdist data-plane listener: GP segments connect here to push rows
// (POST, our read path) or pull rows (GET, our write path).
//
// v1 concurrency model: thread-per-connection with BLOCKING sockets.
// Rationale: segment fan-out per BE is tens of connections at most, the
// session queues (gpfdist_session.h) are blocking by design, and blocking
// code has no event-loop-stall failure modes. If profiling ever shows the
// accept path matters, migrate to libevent bufferevents (the reference
// gpfdist.c model) WITHOUT moving any protocol/session logic - that split is
// exactly why gpfdist_protocol/gpfdist_session are I/O-free.
//
// Protocol invariants served here (see ADB_GPFDIST_PROTOCOL_NOTES.md):
//  - HTTP/1.0, "Connection: close"; GET bodies are frame streams terminated
//    by connection close; POST bodies are Content-Length delimited;
//  - "HTTP/1.0 100 Continue" before reading a POST body when requested;
//  - POST routing: unknown token -> 404, tombstoned token -> 200 (late DONEs
//    must succeed), over-capacity -> 408 (segment retries with backoff),
//    protocol violation -> 400 with a reason the GP user will see;
//  - GET streaming: 200 + per-block F/O/L prelude + D frames, D(len 0) EOF,
//    E frame with message on failure.
//
// TLS (gpfdists): not implemented in v1 - start_once fails loudly when
// config::greenplum_gpfdist_enable_tls is set. The blocking-socket model
// makes the later addition a per-connection OpenSSL BIO wrap, not a rewrite.

namespace starrocks::connector::gpfdist {

class GpfdistServer {
public:
    // Start listening on `port`. Idempotent: subsequent calls return the
    // first call's status. Also starts the session TTL sweeper thread.
    static Status start_once(int32_t port);

    // Stop accepting and join the accept/sweeper threads. In-flight
    // connection threads finish on their own (their sessions get failed by
    // the owning fragments / TTL sweep).
    static void shutdown();

    // Test hook: the actually-bound port (differs from the requested one when
    // tests pass 0 for an ephemeral port). 0 if not started.
    static int32_t bound_port();
};

} // namespace starrocks::connector::gpfdist
