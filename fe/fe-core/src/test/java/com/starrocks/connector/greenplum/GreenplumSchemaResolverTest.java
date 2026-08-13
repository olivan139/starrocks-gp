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

import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class GreenplumSchemaResolverTest {

    @Mocked
    Connection connection;

    @Mocked
    Statement statement;

    @Mocked
    PreparedStatement preparedStatement;

    private GreenplumSchemaResolver resolver;
    private MockResultSet columnResult;

    @BeforeEach
    public void setUp() {
        resolver = new GreenplumSchemaResolver();

        columnResult = new MockResultSet("columns");
        columnResult.addColumn("TABLE_SCHEM", Arrays.asList(
                "gp_schema", "gp_schema", "gp_schema", "gp_schema", "gp_schema",
                "gp_schema", "gp_schema", "gp_schema", "gp_schema", "gp_schema", "gp_schema"));
        columnResult.addColumn("TABLE_NAME", Arrays.asList(
                "tbl", "tbl", "tbl", "tbl", "tbl", "tbl", "tbl", "tbl", "tbl", "tbl", "tbl"));
        // int4, int8, int2, numeric(10,2), varchar(20), text, bool, date, timestamp, float4, float8
        columnResult.addColumn("DATA_TYPE", Arrays.asList(
                Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.NUMERIC, Types.VARCHAR,
                Types.VARCHAR, Types.BIT, Types.DATE, Types.TIMESTAMP, Types.REAL, Types.DOUBLE));
        columnResult.addColumn("TYPE_NAME", Arrays.asList(
                "int4", "int8", "int2", "numeric", "varchar",
                "text", "bool", "date", "timestamp", "float4", "float8"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(
                10, 19, 5, 10, 20,
                2147483647, 1, 13, 29, 8, 17));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(
                0, 0, 0, 2, 0,
                0, 0, 0, 6, 0, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList(
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList(
                "NO", "NO", "YES", "NO", "YES",
                "NO", "YES", "NO", "YES", "NO", "YES"));
    }

    @Test
    public void testConvertToSRTableTypeMapping() throws SQLException {
        List<Column> columns = resolver.convertToSRTable(columnResult);
        Assertions.assertEquals(11, columns.size());

        Assertions.assertTrue(findColumn(columns, "a").getType().isInt());
        Assertions.assertFalse(findColumn(columns, "a").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "b").getType().isBigint());
        Assertions.assertFalse(findColumn(columns, "b").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "c").getType().isSmallint());
        Assertions.assertTrue(findColumn(columns, "c").isAllowNull());

        Type dType = findColumn(columns, "d").getType();
        Assertions.assertTrue(dType.isDecimalOfAnyVersion());
        Assertions.assertEquals(10, ((ScalarType) dType).decimalPrecision());
        Assertions.assertEquals(2, ((ScalarType) dType).decimalScale());
        Assertions.assertFalse(findColumn(columns, "d").isAllowNull());

        Type eType = findColumn(columns, "e").getType();
        Assertions.assertTrue(eType.isVarchar());
        Assertions.assertEquals(20, ((ScalarType) eType).getLength());
        Assertions.assertTrue(findColumn(columns, "e").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "f").getType().isVarchar());
        Assertions.assertFalse(findColumn(columns, "f").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "g").getType().isBoolean());
        Assertions.assertTrue(findColumn(columns, "g").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "h").getType().isDate());
        Assertions.assertFalse(findColumn(columns, "h").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "i").getType().isDatetime());
        Assertions.assertTrue(findColumn(columns, "i").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "j").getType().isFloat());
        Assertions.assertFalse(findColumn(columns, "j").isAllowNull());

        Assertions.assertTrue(findColumn(columns, "k").getType().isDouble());
        Assertions.assertTrue(findColumn(columns, "k").isAllowNull());
    }

    private Column findColumn(List<Column> columns, String name) {
        return columns.stream().filter(c -> c.getName().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("column " + name + " not found"));
    }

    @Test
    public void testListSchemasFiltersSystemSchemas() throws SQLException {
        MockResultSet schemaResult = new MockResultSet("schemas");
        schemaResult.addColumn("schema_name", Arrays.asList("public", "gp_schema"));

        new Expectations() {
            {
                connection.createStatement();
                result = statement;
                minTimes = 0;

                statement.executeQuery(anyString);
                result = schemaResult;
                minTimes = 0;
            }
        };

        Collection<String> schemas = resolver.listSchemas(connection);
        Assertions.assertEquals(2, schemas.size());
        Assertions.assertTrue(schemas.contains("public"));
        Assertions.assertTrue(schemas.contains("gp_schema"));
        Assertions.assertFalse(schemas.contains("pg_catalog"));
        Assertions.assertFalse(schemas.contains("information_schema"));

        // The exclusion is enforced by the SQL sent to the server; assert the resolver actually
        // asks the server to filter out the system schemas.
        new Verifications() {
            {
                String sql;
                statement.executeQuery(sql = withCapture());
                Assertions.assertTrue(sql.contains("NOT IN ('information_schema', 'pg_catalog')"), sql);
                Assertions.assertTrue(sql.contains("pg_toast%"), sql);
                Assertions.assertTrue(sql.contains("pg_temp%"), sql);
            }
        };
    }

    @Test
    public void testGetDistributionColumnsReturnsEmptyOnFailure() throws SQLException {
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = new SQLException("relation \"gp_distribution_policy\" does not exist");
                minTimes = 0;
            }
        };

        Optional<List<String>> result = resolver.getDistributionColumns(connection, "gp_schema", "tbl");
        Assertions.assertTrue(result.isEmpty());
    }
}
