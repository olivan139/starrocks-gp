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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.common.SchemaConstants;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.max;

/**
 * Resolves schema/table/column metadata for a Greenplum catalog.
 * <p>
 * This class is standalone (it does not extend the generic {@code JDBCSchemaResolver}) so the
 * Greenplum connector package stays self-contained. The column type mapping mirrors
 * {@code com.starrocks.connector.jdbc.PostgresSchemaResolver} since Greenplum is
 * PostgreSQL-wire-compatible.
 */
public class GreenplumSchemaResolver {

    private static final Logger LOG = LogManager.getLogger(GreenplumSchemaResolver.class);

    private static final String[] TABLE_TYPES =
            new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "PARTITIONED TABLE"};

    public Collection<String> listSchemas(Connection connection) {
        String sql = "SELECT schema_name FROM information_schema.schemata "
                + "WHERE schema_name NOT IN ('information_schema', 'pg_catalog') "
                + "AND schema_name NOT LIKE 'pg_toast%' AND schema_name NOT LIKE 'pg_temp%'";
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                schemaNames.add(resultSet.getString("schema_name"));
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException("list schemas for Greenplum catalog fail!", e);
        }
    }

    public ResultSet getTables(Connection connection, String schemaName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), schemaName, null, TABLE_TYPES);
    }

    public ResultSet getColumns(Connection connection, String schemaName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), schemaName, tblName, "%");
    }

    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            int dataType = columnSet.getInt("DATA_TYPE");
            String columnName = columnSet.getString("COLUMN_NAME");
            Type type = convertColumnType(dataType,
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            String comment = "";
            // Add try-catch to prevent exceptions when the metadata of some databases does not contain REMARKS
            try {
                if (columnSet.getString("REMARKS") != null) {
                    comment = columnSet.getString("REMARKS");
                }
            } catch (SQLException ignored) {
            }

            fullSchema.add(new Column(normalizeColumnName(columnName), type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES), comment));
        }
        return fullSchema;
    }

    private String normalizeColumnName(String columnName) {
        if (!columnName.equals(columnName.toLowerCase())) {
            return "\"" + columnName + "\"";
        }
        return columnName;
    }

    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.REAL:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
                return TypeFactory.createCharType(columnSize);
            case Types.VARCHAR:
                if ("varchar".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(columnSize);
                } else if ("text".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                primitiveType = PrimitiveType.TIME;
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.OTHER:
                if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.JSON;
                    break;
                } else if ("uuid".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarbinary(columnSize);
                } else if ("time".equalsIgnoreCase(typeName)
                        || "time without time zone".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimeWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimestampWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.DATETIME;
                    break;
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if ("uuid".equalsIgnoreCase(typeName)) {
            return TypeFactory.createVarbinary(columnSize);
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return TypeFactory.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            // if user not specify numeric precision and scale, the default value is 0,
            // we can't defer the precision and scale, can only deal it as string.
            if (precision == 0) {
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
            }
            return TypeFactory.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    private static boolean isTimeWithTimezoneTypeName(String typeName) {
        return "timetz".equalsIgnoreCase(typeName) || "time with time zone".equalsIgnoreCase(typeName);
    }

    private static boolean isTimestampWithTimezoneTypeName(String typeName) {
        return "timestamptz".equalsIgnoreCase(typeName) || "timestamp with time zone".equalsIgnoreCase(typeName);
    }

    /**
     * Detects whether the connected server is Greenplum (vs. plain PostgreSQL) by inspecting
     * {@code SELECT version()}. Greenplum-specific queries (segment count, distribution columns)
     * are gated on this check so the resolver degrades gracefully on plain PostgreSQL.
     */
    public boolean isGreenplum(Connection connection) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT version()")) {
            if (resultSet.next()) {
                String version = resultSet.getString(1);
                return version != null && version.contains("Greenplum");
            }
            return false;
        } catch (SQLException e) {
            LOG.warn("Failed to detect Greenplum server version, assuming plain PostgreSQL", e);
            return false;
        }
    }

    /**
     * Number of primary segments in the Greenplum cluster. Returns 1 when the server is plain
     * PostgreSQL, or when the segment configuration cannot be queried for any reason.
     */
    public int getSegmentCount(Connection connection) {
        if (!isGreenplum(connection)) {
            return 1;
        }
        String sql = "SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0";
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                return count > 0 ? count : 1;
            }
            return 1;
        } catch (SQLException e) {
            LOG.warn("Failed to get Greenplum segment count, falling back to 1", e);
            return 1;
        }
    }

    /**
     * Greenplum-only: returns the table's distribution columns (Greenplum 6 {@code distkey}
     * layout). Returns {@link Optional#empty()} on any failure, including on plain PostgreSQL
     * where {@code gp_distribution_policy} does not exist, so callers can treat the table as
     * randomly distributed.
     */
    public Optional<List<String>> getDistributionColumns(Connection connection, String schema, String table) {
        String sql = "SELECT a.attname FROM gp_distribution_policy p "
                + "JOIN pg_class c ON c.oid = p.localoid "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(p.distkey) "
                + "WHERE n.nspname = ? AND c.relname = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<String> columns = Lists.newArrayList();
                while (resultSet.next()) {
                    columns.add(resultSet.getString("attname"));
                }
                return Optional.of(columns);
            }
        } catch (SQLException e) {
            LOG.debug("Failed to get Greenplum distribution columns for {}.{}, "
                    + "treating table as randomly distributed", schema, table, e);
            return Optional.empty();
        }
    }
}
