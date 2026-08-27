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

package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.connector.greenplum.GreenplumConnectorConstants;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TGreenplumTableSink;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumTableSinkTest {

    private GreenplumTable table;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put(GreenplumConnectorConstants.HOST, "127.0.0.1");
        properties.put(GreenplumConnectorConstants.PORT, GreenplumConnectorConstants.DEFAULT_PORT);
        properties.put(GreenplumConnectorConstants.DATABASE, "gpdb");
        properties.put(GreenplumConnectorConstants.USER, "gpadmin");
        properties.put(GreenplumConnectorConstants.PASSWORD, "123456");
        List<Column> schema = Arrays.asList(new Column("a", IntegerType.INT), new Column("b", VarcharType.VARCHAR),
                new Column("c", DateType.DATE));
        table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
    }

    @Test
    public void testToThrift() {
        GreenplumTableSink sink = new GreenplumTableSink(table, "tok_abc", "\t", "\\N", "gpfdist");

        Assertions.assertEquals("tok_abc", sink.getSessionToken());
        Assertions.assertNull(sink.getExchNodeId());
        Assertions.assertNull(sink.getOutputPartition());

        TDataSink tDataSink = sink.toThrift();
        Assertions.assertEquals(TDataSinkType.GREENPLUM_TABLE_SINK, tDataSink.getType());
        Assertions.assertTrue(tDataSink.isSetGreenplum_table_sink());

        TGreenplumTableSink tSink = tDataSink.getGreenplum_table_sink();
        Assertions.assertEquals("gp_schema", tSink.getSchema_name());
        Assertions.assertEquals("tbl", tSink.getTable_name());
        Assertions.assertEquals(Arrays.asList("a", "b", "c"), tSink.getColumn_names());
        Assertions.assertEquals("tok_abc", tSink.getSession_token());
        Assertions.assertEquals("\t", tSink.getColumn_separator());
        Assertions.assertEquals("\\N", tSink.getNull_marker());
        Assertions.assertEquals("gpfdist", tSink.getTransport_scheme());
    }

    @Test
    public void testGetExplainString() {
        GreenplumTableSink sink = new GreenplumTableSink(table, "tok_abc", "\t", "\\N", "gpfdists");
        String explain = sink.getExplainString("  ", TExplainLevel.NORMAL);
        Assertions.assertTrue(explain.contains("GREENPLUM TABLE SINK"));
        Assertions.assertTrue(explain.contains("gp_schema.tbl"));
        Assertions.assertTrue(explain.contains("gpfdists"));
    }
}
