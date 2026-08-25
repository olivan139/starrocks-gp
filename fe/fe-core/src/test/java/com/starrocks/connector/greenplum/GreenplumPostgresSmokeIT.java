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

package com.starrocks.connector.greenplum;

import com.starrocks.catalog.GreenplumTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live smoke test against a local PostgreSQL (stand-in for Greenplum's
 * PostgreSQL-compatible metadata surface). Not part of the regular UT suite:
 * it only runs when GREENPLUM_SMOKE_HOST is set, e.g.
 *
 *   docker run -d --name gp-smoke -e POSTGRES_PASSWORD=smoke -e POSTGRES_USER=smoke \
 *       -e POSTGRES_DB=smokedb -p 5455:5432 postgres:14
 *   GREENPLUM_SMOKE_HOST=127.0.0.1 GREENPLUM_SMOKE_PORT=5455 \
 *       mvn test -pl fe-core -Dtest=GreenplumPostgresSmokeIT
 *
 * GP-only paths (gp_distribution_policy, gp_segment_configuration) are
 * expected to degrade gracefully here; they are asserted only for graceful
 * fallback, not for content.
 */
public class GreenplumPostgresSmokeIT {

    private static GreenplumMetadata metadata;

    @BeforeAll
    public static void setUp() {
        String host = System.getenv("GREENPLUM_SMOKE_HOST");
        Assumptions.assumeTrue(host != null && !host.isEmpty(),
                "GREENPLUM_SMOKE_HOST not set; skipping live smoke test");

        Map<String, String> properties = new HashMap<>();
        properties.put(GreenplumConnectorConstants.HOST, host);
        properties.put(GreenplumConnectorConstants.PORT,
                System.getenv().getOrDefault("GREENPLUM_SMOKE_PORT", "5455"));
        properties.put(GreenplumConnectorConstants.DATABASE,
                System.getenv().getOrDefault("GREENPLUM_SMOKE_DB", "smokedb"));
        properties.put(GreenplumConnectorConstants.USER,
                System.getenv().getOrDefault("GREENPLUM_SMOKE_USER", "smoke"));
        properties.put(GreenplumConnectorConstants.PASSWORD,
                System.getenv().getOrDefault("GREENPLUM_SMOKE_PASSWORD", "smoke"));
        metadata = new GreenplumMetadata(properties, "gp_smoke");
    }

    @Test
    public void testListDbNamesReturnsSchemas() {
        List<String> dbs = metadata.listDbNames(null);
        assertTrue(dbs.contains("dwh"), "expected schema 'dwh' in " + dbs);
        assertTrue(dbs.contains("public"));
        assertTrue(dbs.stream().noneMatch(s -> s.startsWith("pg_")), "system schemas must be filtered: " + dbs);
    }

    @Test
    public void testGetTableSchema() {
        Table table = metadata.getTable(null, "dwh", "fact");
        assertNotNull(table);
        assertTrue(table instanceof GreenplumTable);
        assertEquals(7, table.getFullSchema().size());
        assertEquals("INT", table.getColumn("id").getType().toString().toUpperCase());
        assertTrue(table.getColumn("name").getType().isStringType());
        assertTrue(table.getColumn("amount").getType().isDecimalOfAnyVersion());
        assertTrue(table.getColumn("dt").getType().isDate());
        assertTrue(table.getColumn("ts").getType().isDatetime());
    }

    @Test
    public void testGetTableMissingReturnsNull() {
        assertTrue(metadata.getTable(null, "dwh", "no_such_table") == null);
    }

    @Test
    public void testStatisticsFromPgCatalog() {
        Table table = metadata.getTable(null, "dwh", "fact");
        assertNotNull(table);
        Statistics stats = metadata.getTableStatistics(null, table,
                new HashMap<>(), null, null, -1, null);
        // ANALYZE ran on 50k rows: reltuples must be far from both 0 and the
        // FE default constant (1) — allow estimation slack.
        assertTrue(stats.getOutputRowCount() > 40000 && stats.getOutputRowCount() < 60000,
                "row count from pg_class.reltuples expected ~50000, got " + stats.getOutputRowCount());
    }

    @Test
    public void testSegmentCountFallsBackOnPlainPostgres() {
        // plain Postgres has no gp_segment_configuration: graceful fallback to 1
        assertEquals(1, metadata.getSegmentCount());
    }

    /**
     * Live check of the write-path orchestrator's JDBC plumbing. Plain
     * PostgreSQL cannot accept CREATE READABLE EXTERNAL TABLE (GP-only
     * syntax), so the expected outcome is a clean, user-facing failure —
     * which exercises: connection open (with TimeZone=UTC options), the
     * pre-transaction segment-count probe degrading gracefully on non-GP,
     * transaction begin, DDL dispatch, error propagation through
     * Handle.finish(), rollback and connection close.
     */
    @Test
    public void testOrchestratorErrorSurfaceOnPlainPostgres() {
        GreenplumTable table = (GreenplumTable) metadata.getTable(null, "dwh", "fact");
        assertNotNull(table);
        GreenplumLoadOrchestrator.Handle handle = GreenplumLoadOrchestrator.start(
                table, "smoke_token", java.util.Collections.singletonList("127.0.0.1"), 30);
        try {
            handle.finish(0);
            org.junit.jupiter.api.Assertions.fail("expected failure: plain PostgreSQL cannot create "
                    + "a readable external table");
        } catch (Exception e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            assertTrue(msg.contains("greenplum load failed"),
                    "expected a wrapped greenplum load error, got: " + msg);
        }
    }
}
