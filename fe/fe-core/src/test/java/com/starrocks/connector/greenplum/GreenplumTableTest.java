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
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TGreenplumTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumTableTest {

    private Map<String, String> properties;
    private List<Column> schema;

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>();
        properties.put(GreenplumConnectorConstants.HOST, "127.0.0.1");
        properties.put(GreenplumConnectorConstants.PORT, "5432");
        properties.put(GreenplumConnectorConstants.DATABASE, "gpdb");
        properties.put(GreenplumConnectorConstants.USER, "gpadmin");
        properties.put(GreenplumConnectorConstants.PASSWORD, "123456");
        schema = Collections.singletonList(new Column("a", IntegerType.INT));
    }

    @Test
    public void testConstructorValidatesNullProperties() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", null));
    }

    @Test
    public void testConstructorValidatesMissingHost() {
        properties.remove(GreenplumConnectorConstants.HOST);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties));
    }

    @Test
    public void testConstructorValidatesMissingDatabase() {
        properties.remove(GreenplumConnectorConstants.DATABASE);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties));
    }

    @Test
    public void testConstructorValidatesMissingUser() {
        properties.remove(GreenplumConnectorConstants.USER);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties));
    }

    @Test
    public void testConstructorValidatesMissingPassword() {
        properties.remove(GreenplumConnectorConstants.PASSWORD);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties));
    }

    @Test
    public void testGetters() {
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        Assertions.assertEquals("127.0.0.1", table.getHost());
        Assertions.assertEquals("5432", table.getPort());
        Assertions.assertEquals("gpdb", table.getDatabase());
        Assertions.assertEquals("copy", table.getTransport());
        Assertions.assertEquals("greenplum0", table.getCatalogName());
        Assertions.assertEquals("gp_schema", table.getCatalogDBName());
        Assertions.assertEquals("tbl", table.getCatalogTableName());
        Assertions.assertEquals(Table.TableType.GREENPLUM, table.getType());
    }

    @Test
    public void testTransportDefaultsToCopyWhenOmitted() {
        Assertions.assertNull(properties.get(GreenplumConnectorConstants.TRANSPORT));
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        Assertions.assertEquals(GreenplumConnectorConstants.TRANSPORT_COPY, table.getTransport());
    }

    @Test
    public void testTransportHonorsExplicitValue() {
        properties.put(GreenplumConnectorConstants.TRANSPORT, "gpfdist");
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        Assertions.assertEquals("gpfdist", table.getTransport());
    }

    @Test
    public void testPortDefaultsWhenOmitted() {
        properties.remove(GreenplumConnectorConstants.PORT);
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        Assertions.assertEquals(GreenplumConnectorConstants.DEFAULT_PORT, table.getPort());
    }

    @Test
    public void testGetUUIDFormatIsCatalogDbTable() {
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        Assertions.assertEquals("greenplum0.gp_schema.tbl", table.getUUID());
    }

    @Test
    public void testToThrift() {
        GreenplumTable table = new GreenplumTable(1L, "tbl", schema, "gp_schema", "greenplum0", properties);
        TTableDescriptor descriptor = table.toThrift(null);
        Assertions.assertEquals(TTableType.GREENPLUM_TABLE, descriptor.getTableType());
        Assertions.assertTrue(descriptor.isSetGreenplumTable());

        TGreenplumTable gpTable = descriptor.getGreenplumTable();
        Assertions.assertEquals("127.0.0.1", gpTable.getHost());
        Assertions.assertEquals("5432", gpTable.getPort());
        Assertions.assertEquals("gpdb", gpTable.getDatabase());
        Assertions.assertEquals("gpadmin", gpTable.getUser());
        Assertions.assertEquals("123456", gpTable.getPasswd());
    }
}
