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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumLoadOrchestratorTest {

    private GreenplumTable table;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put(GreenplumConnectorConstants.HOST, "127.0.0.1");
        properties.put(GreenplumConnectorConstants.PORT, GreenplumConnectorConstants.DEFAULT_PORT);
        properties.put(GreenplumConnectorConstants.DATABASE, "gpdb");
        properties.put(GreenplumConnectorConstants.USER, "gpadmin");
        properties.put(GreenplumConnectorConstants.PASSWORD, "123456");
        List<Column> schema = Collections.singletonList(new Column("a", IntegerType.INT));
        table = new GreenplumTable(1L, "t", schema, "s", "greenplum0", properties);
    }

    /** Builds a Handle without calling {@code start()}/{@code submit()}, so no thread or JDBC connection is created. */
    private GreenplumLoadOrchestrator.Handle newHandle(String token, List<String> sinkHosts) throws Exception {
        Constructor<GreenplumLoadOrchestrator.Handle> ctor = GreenplumLoadOrchestrator.Handle.class
                .getDeclaredConstructor(GreenplumTable.class, String.class, List.class, int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(table, token, sinkHosts, 60);
    }

    @Test
    public void testSessionTokenIsStableAndUrlSafe() {
        TUniqueId id = new TUniqueId(123L, 456L);
        String token1 = GreenplumLoadOrchestrator.sessionToken(id);
        String token2 = GreenplumLoadOrchestrator.sessionToken(new TUniqueId(123L, 456L));

        Assertions.assertEquals(token1, token2);
        Assertions.assertFalse(token1.contains("-"));
    }

    @Test
    public void testSessionTokenDiffersForDifferentIds() {
        String token1 = GreenplumLoadOrchestrator.sessionToken(new TUniqueId(1L, 2L));
        String token2 = GreenplumLoadOrchestrator.sessionToken(new TUniqueId(3L, 4L));
        Assertions.assertNotEquals(token1, token2);
    }

    @Test
    public void testBuildLocationsDefaultPortAndScheme() throws Exception {
        GreenplumLoadOrchestrator.Handle handle = newHandle("tok", Arrays.asList("host1", "host2"));
        List<String> locations = handle.buildLocations();
        Assertions.assertEquals(Arrays.asList("'gpfdist://host1:8907/tok'", "'gpfdist://host2:8907/tok'"), locations);
    }

    @Test
    public void testBuildLocationsHonorsCustomPortAndScheme() throws Exception {
        Map<String, String> properties = new HashMap<>(table.getConnectInfo());
        properties.put(GreenplumConnectorConstants.GPFDIST_PORT, "9000");
        properties.put(GreenplumConnectorConstants.TRANSPORT_SCHEME, "gpfdists");
        GreenplumTable customTable = new GreenplumTable(1L, "t", Collections.singletonList(new Column("a", IntegerType.INT)),
                "s", "greenplum0", properties);
        table = customTable;

        GreenplumLoadOrchestrator.Handle handle = newHandle("tok", Collections.singletonList("host1"));
        List<String> locations = handle.buildLocations();
        Assertions.assertEquals(Collections.singletonList("'gpfdists://host1:9000/tok'"), locations);
    }

    @Test
    public void testBuildCreateExternalTableSqlGolden() throws Exception {
        GreenplumLoadOrchestrator.Handle handle = newHandle("tok", Collections.singletonList("h"));
        String sql = handle.buildCreateExternalTableSql("\"s\".\"ext_sr_tok\"", "\"s\".\"t\"");

        Assertions.assertTrue(sql.contains("CREATE READABLE EXTERNAL TABLE \"s\".\"ext_sr_tok\""));
        Assertions.assertTrue(sql.contains("(LIKE \"s\".\"t\")"));
        Assertions.assertTrue(sql.contains("LOCATION('gpfdist://h:8907/tok')"));
        Assertions.assertTrue(sql.contains("FORMAT 'TEXT'"));
        // DEFAULT_COLUMN_SEPARATOR is a raw tab char (no backslash/quote to escape).
        Assertions.assertTrue(sql.contains("DELIMITER E'" + GreenplumConnectorConstants.DEFAULT_COLUMN_SEPARATOR + "'"));
        // DEFAULT_NULL_MARKER ("\N") gets its backslash doubled by the SQL-escaper.
        String expectedNullClause = "NULL E'"
                + GreenplumConnectorConstants.DEFAULT_NULL_MARKER.replace("\\", "\\\\") + "'";
        Assertions.assertTrue(sql.contains(expectedNullClause));
    }

    @Test
    public void testAbortBeforeStartIsSafeAndIdempotent() throws Exception {
        GreenplumLoadOrchestrator.Handle handle = newHandle("tok", Collections.singletonList("host1"));
        Assertions.assertDoesNotThrow(() -> handle.abort("first abort"));
        Assertions.assertDoesNotThrow(() -> handle.abort("second abort"));
    }
}
