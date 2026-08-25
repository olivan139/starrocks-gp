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

#include <gtest/gtest.h>

#include <thread>

namespace starrocks::connector::gpfdist {

// ---- PushSession: the seq/dedupe state machine (write-to-us) ----

TEST(PushSessionTest, open_accept_done_flow) {
    PushSession s("tok", 1 << 20);
    // seq 1 = open probe, nothing buffered
    auto a = s.on_post(0, 1, false, "");
    ASSERT_TRUE(a.ok());
    EXPECT_EQ(PostAction::ACK_OPEN, *a);
    // data posts in order
    ASSERT_EQ(PostAction::ACCEPT, *s.on_post(0, 2, false, "r1\n"));
    ASSERT_EQ(PostAction::ACCEPT, *s.on_post(0, 3, false, "r2\n"));
    EXPECT_FALSE(s.all_segments_done());
    // final empty DONE
    ASSERT_EQ(PostAction::ACK_DONE, *s.on_post(0, 0, true, ""));
    EXPECT_TRUE(s.all_segments_done());

    auto d1 = s.take(1000);
    ASSERT_TRUE(d1.ok());
    EXPECT_EQ("r1\n", d1->value());
    auto d2 = s.take(1000);
    ASSERT_TRUE(d2.ok());
    EXPECT_EQ("r2\n", d2->value());
    auto eof = s.take(1000);
    ASSERT_TRUE(eof.ok());
    EXPECT_FALSE(eof->has_value()); // clean EOF
}

TEST(PushSessionTest, duplicate_seq_is_acked_and_dropped) {
    // THE critical protocol rule: segments RESEND the same seq after network
    // errors; appending it twice would silently duplicate loaded data.
    PushSession s("tok", 1 << 20);
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(1, 1, false, ""));
    ASSERT_EQ(PostAction::ACCEPT, *s.on_post(1, 2, false, "payload"));
    ASSERT_EQ(PostAction::ACK_DUPLICATE, *s.on_post(1, 2, false, "payload"));
    ASSERT_EQ(PostAction::ACK_DONE, *s.on_post(1, 0, true, ""));
    ASSERT_EQ("payload", s.take(1000)->value());
    EXPECT_FALSE(s.take(1000)->has_value()); // exactly once
}

TEST(PushSessionTest, out_of_order_seq_is_an_error) {
    PushSession s("tok", 1 << 20);
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(1, 1, false, ""));
    EXPECT_FALSE(s.on_post(1, 4, false, "skipped ahead").ok()); // gap -> 400
}

TEST(PushSessionTest, multiple_segments_eof_needs_all_done) {
    PushSession s("tok", 1 << 20);
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(0, 1, false, ""));
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(7, 1, false, ""));
    ASSERT_EQ(PostAction::ACCEPT, *s.on_post(7, 2, false, "a"));
    ASSERT_EQ(PostAction::ACK_DONE, *s.on_post(7, 0, true, ""));
    EXPECT_FALSE(s.all_segments_done()); // segment 0 still open
    ASSERT_EQ(PostAction::ACK_DONE, *s.on_post(0, 0, true, ""));
    EXPECT_TRUE(s.all_segments_done());
    // DONE from a segment we never saw data from is legal
    ASSERT_EQ(PostAction::ACK_DONE, *s.on_post(3, 0, true, ""));
}

TEST(PushSessionTest, capacity_and_fail) {
    PushSession s("tok", 4); // tiny bound
    EXPECT_FALSE(s.over_capacity(3));
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(0, 1, false, ""));
    ASSERT_EQ(PostAction::ACCEPT, *s.on_post(0, 2, false, "abcd"));
    EXPECT_TRUE(s.over_capacity(1)); // server should now answer 408

    s.fail(Status::Aborted("query cancelled"));
    EXPECT_FALSE(s.take(1000).ok());
    EXPECT_FALSE(s.on_post(0, 3, false, "x").ok());
}

TEST(PushSessionTest, take_blocks_until_data_arrives) {
    PushSession s("tok", 1 << 20);
    ASSERT_EQ(PostAction::ACK_OPEN, *s.on_post(0, 1, false, ""));
    std::thread producer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        (void)s.on_post(0, 2, false, "late");
    });
    auto got = s.take(5000);
    producer.join();
    ASSERT_TRUE(got.ok());
    EXPECT_EQ("late", got->value());
}

// ---- PullSession: block handout (read-from-us) ----

TEST(PullSessionTest, each_block_to_exactly_one_consumer) {
    PullSession s("tok", 1 << 20);
    ASSERT_TRUE(s.put_block("b0", 1000).ok());
    ASSERT_TRUE(s.put_block("b1", 1000).ok());
    s.finish();
    auto a = s.next_block(1000);
    auto b = s.next_block(1000);
    auto c = s.next_block(1000);
    ASSERT_TRUE(a.ok() && b.ok() && c.ok());
    EXPECT_EQ("b0", a->value());
    EXPECT_EQ("b1", b->value());
    EXPECT_FALSE(c->has_value()); // EOF after drain
}

TEST(PullSessionTest, consumer_timeout_maps_to_408) {
    PullSession s("tok", 1 << 20);
    auto r = s.next_block(20);
    ASSERT_FALSE(r.ok());
    EXPECT_TRUE(r.status().is_time_out());
}

TEST(PullSessionTest, producer_backpressure_and_fail) {
    PullSession s("tok", 1); // one byte: second block must wait
    ASSERT_TRUE(s.put_block("xx", 100).ok()); // first block always admitted
    auto blocked = s.put_block("yy", 50);
    EXPECT_TRUE(blocked.is_time_out());
    s.fail(Status::Aborted("cancelled"));
    EXPECT_FALSE(s.put_block("zz", 50).ok());
    EXPECT_FALSE(s.next_block(50).ok());
}

// ---- Registry ----

TEST(SessionRegistryTest, lifecycle_tombstones_sweep) {
    auto* reg = SessionRegistry::instance();
    auto push = reg->create_push("t1", 1 << 20);
    ASSERT_NE(nullptr, push);
    EXPECT_EQ(push.get(), reg->find_push("t1").get());
    EXPECT_EQ(nullptr, reg->find_pull("t2"));

    reg->remove("t1", /*now_ms=*/1000);
    EXPECT_EQ(nullptr, reg->find_push("t1"));
    EXPECT_TRUE(reg->is_tombstoned("t1")); // late X-GP-DONE -> 200, not 404

    reg->sweep(/*now_ms=*/1000 + 60'000, /*ttl_ms=*/30'000);
    EXPECT_FALSE(reg->is_tombstoned("t1"));

    // idle session reaping: touched long ago -> swept + consumer unblocked
    auto stale = reg->create_pull("t3", 1 << 20);
    reg->touch("t3", 2000);
    reg->sweep(/*now_ms=*/2000 + 60'000, /*ttl_ms=*/30'000);
    EXPECT_EQ(nullptr, reg->find_pull("t3"));
    EXPECT_FALSE(stale->next_block(10).ok()); // failed, not hung
}

} // namespace starrocks::connector::gpfdist
