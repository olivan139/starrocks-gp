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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal {@link ConnectorMetadata} mock for Greenplum plan tests: a single catalog with one
 * schema ({@link #MOCKED_DB_NAME}) and two tables.
 */
public class MockedGreenplumMetadata implements ConnectorMetadata {

    public static final String MOCKED_GREENPLUM_CATALOG_NAME = "greenplum0";
    public static final String MOCKED_DB_NAME = "gp_schema";
    public static final String MOCKED_TABLE_NAME0 = "t0";
    public static final String MOCKED_TABLE_NAME1 = "t1";

    public static final Map<String, String> VALID_PROPERTIES = ImmutableMap.<String, String>builder()
            .put(GreenplumConnectorConstants.HOST, "127.0.0.1")
            .put(GreenplumConnectorConstants.PORT, GreenplumConnectorConstants.DEFAULT_PORT)
            .put(GreenplumConnectorConstants.DATABASE, "gpdb")
            .put(GreenplumConnectorConstants.USER, "gpadmin")
            .put(GreenplumConnectorConstants.PASSWORD, "123456")
            .build();

    private final Map<String, Table> tables = new HashMap<>();

    private List<Column> getSchema(String tblName) {
        if (tblName.equals(MOCKED_TABLE_NAME1)) {
            return Arrays.asList(new Column("id", IntegerType.INT), new Column("v", FloatType.DOUBLE));
        }
        return Arrays.asList(new Column("a", IntegerType.INT), new Column("b", VarcharType.VARCHAR),
                new Column("c", DateType.DATE));
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.GREENPLUM;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return tables.computeIfAbsent(tblName, name -> new GreenplumTable(name.hashCode(), name,
                getSchema(name), MOCKED_DB_NAME, MOCKED_GREENPLUM_CATALOG_NAME, new HashMap<>(VALID_PROPERTIES)));
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (!MOCKED_DB_NAME.equals(dbName)) {
            return null;
        }
        return new Database(0, MOCKED_DB_NAME);
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return Arrays.asList(MOCKED_DB_NAME);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return Arrays.asList(MOCKED_TABLE_NAME0, MOCKED_TABLE_NAME1);
    }
}
