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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.greenplum.credential.GreenplumCredential;
import com.starrocks.connector.greenplum.credential.GreenplumCredentialProvider;
import com.starrocks.connector.greenplum.credential.StaticGreenplumCredentialProvider;
import com.starrocks.connector.jdbc.JDBCMetaCache;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(GreenplumMetadata.class);

    // Conservative pool sizing: Greenplum queries are typically metadata/aggregation-only from
    // FE's perspective, and each Greenplum segment host can only accept a limited number of
    // concurrent connections from the coordinator.
    private static final int MAX_POOL_SIZE = 10;
    private static final int MIN_IDLE = 1;

    private final Map<String, String> properties;
    private final String catalogName;
    private final GreenplumSchemaResolver schemaResolver;
    private final GreenplumCredentialProvider credentialProvider;
    private final HikariDataSource dataSource;

    private final JDBCMetaCache<String, Database> dbCache;
    private final JDBCMetaCache<String, Table> tableCache;
    private final JDBCMetaCache<String, Long> tableIdCache;
    private final JDBCMetaCache<String, RawTableStats> statsCache;

    private volatile Integer segmentCount;

    public GreenplumMetadata(Map<String, String> properties, String catalogName) {
        this.properties = properties;
        this.catalogName = catalogName;
        this.schemaResolver = new GreenplumSchemaResolver();
        this.credentialProvider = new StaticGreenplumCredentialProvider(properties);
        this.dataSource = createHikariDataSource();
        this.dbCache = new JDBCMetaCache<>(properties, false);
        this.tableCache = new JDBCMetaCache<>(properties, false);
        this.tableIdCache = new JDBCMetaCache<>(properties, true);
        this.statsCache = new JDBCMetaCache<>(properties, false);
    }

    private HikariDataSource createHikariDataSource() {
        String host = properties.get(GreenplumConnectorConstants.HOST);
        String port = properties.getOrDefault(GreenplumConnectorConstants.PORT,
                GreenplumConnectorConstants.DEFAULT_PORT);
        String database = properties.get(GreenplumConnectorConstants.DATABASE);
        String jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        GreenplumCredential credential = credentialProvider.get();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName("org.postgresql.Driver");
        config.setUsername(credential.getUser());
        config.setPassword(credential.getPassword());
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setMinimumIdle(MIN_IDLE);

        return new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.GREENPLUM;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        try (Connection connection = getConnection()) {
            return Lists.newArrayList(schemaResolver.listSchemas(connection));
        } catch (SQLException e) {
            throw new StarRocksConnectorException("list db names for Greenplum catalog fail!", e);
        }
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        // Manual cache control (getIfPresent + put) instead of the lambda-based JDBCMetaCache#get,
        // because a non-existent database is a valid (null) result and must not be cached as such;
        // see JDBCMetadata#getDb for the identical rationale.
        Database cached = dbCache.getIfPresent(name);
        if (cached != null) {
            return cached;
        }

        try (Connection connection = getConnection()) {
            if (schemaResolver.listSchemas(connection).contains(name)) {
                Database db = new Database(0, name);
                dbCache.put(name, db);
                return db;
            }
            return null;
        } catch (SQLException e) {
            LOG.warn("Failed to check database existence for {}.{}: {}", catalogName, name, e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try (Connection connection = getConnection()) {
            try (ResultSet resultSet = schemaResolver.getTables(connection, dbName)) {
                ImmutableList.Builder<String> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(resultSet.getString("TABLE_NAME"));
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException("list table names for Greenplum catalog fail!", e);
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        String cacheKey = dbName + "." + tblName;
        // NOTE: manual cache control (getIfPresent + put) instead of the lambda-based
        // JDBCMetaCache.get: that method wraps results in Objects.requireNonNull, so a
        // lambda returning null (table does not exist - a valid result, not an error)
        // would throw NPE. Only successful lookups are cached.
        Table cached = tableCache.getIfPresent(cacheKey);
        if (cached != null) {
            return cached;
        }

        try (Connection connection = getConnection();
                ResultSet columnSet = schemaResolver.getColumns(connection, dbName, tblName)) {
            List<Column> fullSchema = schemaResolver.convertToSRTable(columnSet);
            if (fullSchema.isEmpty()) {
                return null;
            }

            Long tableId = tableIdCache.getPersistentCache(cacheKey,
                    j -> (long) ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
            Table table = new GreenplumTable(tableId, tblName, fullSchema, dbName, catalogName, properties);
            tableCache.put(cacheKey, table);
            return table;
        } catch (SQLException e) {
            LOG.warn("get table for Greenplum catalog fail!", e);
            return null;
        }
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        GreenplumTable greenplumTable = (GreenplumTable) table;
        String cacheKey = greenplumTable.getCatalogDBName() + "." + greenplumTable.getName();
        if (!onlyCachedPartitions) {
            tableCache.invalidate(cacheKey);
        }
    }

    /**
     * Number of primary Greenplum segments, used by the planner to size scan parallelism.
     * Cached after the first successful lookup for the lifetime of this metadata instance.
     */
    public int getSegmentCount() {
        Integer cached = segmentCount;
        if (cached != null) {
            return cached;
        }
        try (Connection connection = getConnection()) {
            int count = schemaResolver.getSegmentCount(connection);
            segmentCount = count;
            return count;
        } catch (SQLException e) {
            LOG.warn("Failed to get Greenplum segment count, falling back to 1", e);
            return 1;
        }
    }

    /**
     * Table statistics for the CBO, sourced from Greenplum's own catalogs:
     * row count from pg_class.reltuples, per-column NDV / null fraction /
     * average width from pg_stats. Both require ANALYZE to have run on the
     * remote table; when absent we fall back to the same default row count
     * the JDBC connector uses, with unknown column statistics, so join
     * planning degrades gracefully instead of trusting a bogus zero.
     */
    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange tableVersionRange) {
        GreenplumTable gpTable = (GreenplumTable) table;
        String cacheKey = gpTable.getCatalogDBName() + "." + gpTable.getCatalogTableName();

        RawTableStats raw = statsCache.getIfPresent(cacheKey);
        if (raw == null) {
            raw = fetchRawTableStats(gpTable.getCatalogDBName(), gpTable.getCatalogTableName());
            statsCache.put(cacheKey, raw);
        }

        Statistics.Builder builder = Statistics.builder();
        double rowCount = raw.rowCount > 0 ? raw.rowCount : Config.default_statistics_output_row_count;
        builder.setOutputRowCount(rowCount);
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            RawColumnStats col = raw.columnStats.get(entry.getValue().getName());
            if (raw.rowCount <= 0 || col == null) {
                builder.addColumnStatistic(entry.getKey(), ColumnStatistic.unknown());
                continue;
            }
            // pg_stats.n_distinct: > 0 is an absolute count, < 0 is a negated
            // fraction of the row count (e.g. -1 means all-distinct).
            double ndv = col.nDistinct >= 0 ? col.nDistinct : -col.nDistinct * rowCount;
            builder.addColumnStatistic(entry.getKey(), ColumnStatistic.builder()
                    .setNullsFraction(col.nullFraction)
                    .setDistinctValuesCount(Math.max(1, ndv))
                    .setAverageRowSize(col.avgWidth)
                    .build());
        }
        return builder.build();
    }

    private RawTableStats fetchRawTableStats(String schemaName, String tableName) {
        RawTableStats result = new RawTableStats();
        try (Connection connection = getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT c.reltuples FROM pg_catalog.pg_class c "
                            + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                            + "WHERE n.nspname = ? AND c.relname = ?")) {
                ps.setString(1, schemaName);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        result.rowCount = (long) rs.getDouble(1);
                    }
                }
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT attname, n_distinct, null_frac, avg_width FROM pg_catalog.pg_stats "
                            + "WHERE schemaname = ? AND tablename = ?")) {
                ps.setString(1, schemaName);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        RawColumnStats col = new RawColumnStats();
                        col.nDistinct = rs.getDouble(2);
                        col.nullFraction = rs.getDouble(3);
                        col.avgWidth = rs.getDouble(4);
                        result.columnStats.put(rs.getString(1), col);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to fetch Greenplum statistics for {}.{}, using defaults",
                    schemaName, tableName, e);
        }
        return result;
    }

    private static final class RawTableStats {
        private long rowCount = -1;
        private final Map<String, RawColumnStats> columnStats = new HashMap<>();
    }

    private static final class RawColumnStats {
        private double nDistinct;
        private double nullFraction;
        private double avgWidth;
    }

    @Override
    public void shutdown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
