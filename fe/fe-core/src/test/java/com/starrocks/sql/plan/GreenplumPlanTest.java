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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GreenplumPlanTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @Test
    public void testScan() throws Exception {
        String sql = "SELECT * FROM greenplum0.gp_schema.t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN GREENPLUM");
    }

    @Test
    public void testColumnPruning() throws Exception {
        String sql = "SELECT a FROM greenplum0.gp_schema.t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN GREENPLUM");
        assertContains(plan, "QUERY: SELECT \"a\"");
        Assertions.assertFalse(plan.contains("\"b\""), plan);
    }

    @Test
    public void testPredicatePushdown() throws Exception {
        String sql = "SELECT a FROM greenplum0.gp_schema.t0 WHERE a > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN GREENPLUM");
        assertContains(plan, "WHERE");
        assertContains(plan, "\"a\" > 10");
    }

    @Test
    public void testLimit() throws Exception {
        String sql = "SELECT a FROM greenplum0.gp_schema.t0 LIMIT 7";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN GREENPLUM");
        assertContains(plan, "LIMIT 7");
    }

    @Test
    public void testJoinWithOlap() throws Exception {
        String sql = "SELECT l.a, r.v1 FROM greenplum0.gp_schema.t0 l JOIN test.t0 r ON l.a = r.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN GREENPLUM");
        assertContains(plan, "OlapScanNode");
        assertContains(plan, "JOIN");
    }
}
