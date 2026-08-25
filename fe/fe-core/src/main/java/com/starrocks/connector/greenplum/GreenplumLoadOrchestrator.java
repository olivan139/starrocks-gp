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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Drives the Greenplum side of {@code INSERT INTO greenplum_catalog...}.
 * <p>
 * While the StarRocks fragments execute (BE sinks serving rows over gpfdist),
 * this orchestrator runs, on its own thread, a single Greenplum transaction:
 * <pre>
 *   BEGIN;
 *   CREATE READABLE EXTERNAL TABLE ext_sr_&lt;token&gt; (LIKE target)
 *     LOCATION('gpfdist(s)://be1:port/&lt;token&gt;', ...) FORMAT 'TEXT'(...);
 *   INSERT INTO target SELECT * FROM ext_sr_&lt;token&gt;;   -- segments pull from BEs
 *   DROP EXTERNAL TABLE ext_sr_&lt;token&gt;;
 *   -- (reconcile row counts) COMMIT / ROLLBACK
 * </pre>
 * Because Greenplum DDL is transactional, the external table is never visible
 * outside this transaction and any failure (including FE death: session drop
 * implies rollback) leaves no orphans. COMMIT happens only after the row count
 * reported by Greenplum matches the row count reported by the BE sinks.
 */
public class GreenplumLoadOrchestrator {

    private static final Logger LOG = LogManager.getLogger(GreenplumLoadOrchestrator.class);

    private static final ExecutorService EXECUTOR =
            ThreadPoolManager.newDaemonCacheThreadPool(64, "greenplum-load", true);

    private GreenplumLoadOrchestrator() {
    }

    /** Stable per-query token; also the path component of the gpfdist URLs. */
    public static String sessionToken(TUniqueId executionId) {
        return DebugUtil.printId(executionId).replace("-", "_");
    }

    /**
     * Launch the Greenplum-side transaction concurrently with fragment
     * execution. Must be called after the coordinator deployed fragments
     * (sink hosts are final) and before waiting for completion.
     *
     * @param sinkHosts distinct hosts of the BEs running sink fragments
     */
    public static Handle start(GreenplumTable table, String token, List<String> sinkHosts, int timeoutSec) {
        Preconditions.checkArgument(!sinkHosts.isEmpty(), "no sink hosts");
        Handle handle = new Handle(table, token, sinkHosts, timeoutSec);
        handle.submit();
        return handle;
    }

    public static class Handle {
        private final GreenplumTable table;
        private final String token;
        private final List<String> sinkHosts;
        private final int timeoutSec;
        private final AtomicBoolean aborted = new AtomicBoolean(false);
        private final AtomicBoolean finished = new AtomicBoolean(false);

        private volatile Connection connection;
        private volatile Statement runningStatement;
        private volatile Future<Long> insertedRows;

        private Handle(GreenplumTable table, String token, List<String> sinkHosts, int timeoutSec) {
            this.table = table;
            this.token = token;
            this.sinkHosts = sinkHosts;
            this.timeoutSec = timeoutSec;
        }

        private void submit() {
            insertedRows = EXECUTOR.submit(this::runLoadTransaction);
        }

        /**
         * Called after the StarRocks coordinator finished successfully.
         * Waits for the Greenplum INSERT, reconciles counts, then commits.
         *
         * @param rowsFromSinks sum of rows the BE sinks reported serving
         * @return rows inserted on the Greenplum side
         */
        public long finish(long rowsFromSinks) {
            Preconditions.checkState(insertedRows != null, "orchestrator not started");
            try {
                long gpRows = insertedRows.get(timeoutSec, TimeUnit.SECONDS);
                if (gpRows != rowsFromSinks) {
                    rollbackQuietly();
                    throw new StarRocksConnectorException(String.format(
                            "greenplum load row count mismatch: sinks served %d rows but greenplum inserted %d; "
                                    + "transaction rolled back", rowsFromSinks, gpRows));
                }
                connection.commit();
                finished.set(true);
                LOG.info("greenplum load {} committed, {} rows into {}.{}",
                        token, gpRows, table.getCatalogDBName(), table.getCatalogTableName());
                return gpRows;
            } catch (TimeoutException e) {
                abort("timed out waiting for greenplum INSERT after " + timeoutSec + "s");
                throw new StarRocksConnectorException(
                        "greenplum load timed out after " + timeoutSec + "s; transaction rolled back");
            } catch (StarRocksConnectorException e) {
                throw e;
            } catch (Exception e) {
                abort("greenplum load failed: " + e.getMessage());
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new StarRocksConnectorException("greenplum load failed: " + cause.getMessage(), cause);
            } finally {
                closeQuietly();
            }
        }

        /** Roll back and cancel the in-flight statement. Idempotent, never throws. */
        public void abort(String reason) {
            if (!aborted.compareAndSet(false, true) || finished.get()) {
                return;
            }
            LOG.warn("aborting greenplum load {}: {}", token, reason);
            Statement stmt = runningStatement;
            if (stmt != null) {
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    LOG.warn("failed to cancel greenplum statement for load {}", token, e);
                }
            }
            if (insertedRows != null) {
                insertedRows.cancel(true);
            }
            rollbackQuietly();
            closeQuietly();
        }

        private long runLoadTransaction() throws SQLException {
            Connection conn = openConnection();
            this.connection = conn;
            // probe before the transaction starts: a failed probe inside the txn
            // would abort it on plain PostgreSQL and mask the real error
            try (Statement probe = conn.createStatement()) {
                checkLocationCount(probe);
            }
            conn.setAutoCommit(false);
            String extTable = quote(table.getCatalogDBName()) + "." + quote("ext_sr_" + token);
            String target = quote(table.getCatalogDBName()) + "." + quote(table.getCatalogTableName());
            try (Statement stmt = conn.createStatement()) {
                this.runningStatement = stmt;
                stmt.execute(buildCreateExternalTableSql(extTable, target));
                // Blocks while GP segments pull rows from the BE gpfdist endpoints.
                long rows = stmt.executeLargeUpdate("INSERT INTO " + target + " SELECT * FROM " + extTable);
                stmt.execute("DROP EXTERNAL TABLE " + extTable);
                return rows;
            } finally {
                this.runningStatement = null;
            }
        }

        /**
         * Greenplum rejects a readable external table with more LOCATION URLs
         * than primary segments; fail early with an actionable message instead.
         * Degrades silently on plain PostgreSQL (no gp_segment_configuration).
         */
        private void checkLocationCount(Statement stmt) throws SQLException {
            int segments;
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content >= 0")) {
                segments = rs.next() ? rs.getInt(1) : 0;
            } catch (SQLException e) {
                return; // not a Greenplum (e.g. plain PostgreSQL in tests) - let the DDL decide
            }
            if (segments > 0 && sinkHosts.size() > segments) {
                throw new StarRocksConnectorException(String.format(
                        "greenplum load needs %d gpfdist endpoints (one per StarRocks sink host) but the cluster "
                                + "has only %d primary segments; reduce the number of sink hosts "
                                + "(e.g. lower sink parallelism)", sinkHosts.size(), segments));
            }
        }

        String buildCreateExternalTableSql(String extTable, String target) {
            List<String> locations = buildLocations();
            return "CREATE READABLE EXTERNAL TABLE " + extTable + " (LIKE " + target + ") "
                    + "LOCATION(" + Joiner.on(", ").join(locations) + ") "
                    + "FORMAT 'TEXT' (DELIMITER E'" + escape(table.getColumnSeparator())
                    + "' NULL E'" + escape(table.getNullMarker()) + "')";
        }

        List<String> buildLocations() {
            String scheme = table.getTransportScheme();
            String port = table.getGpfdistPort();
            return sinkHosts.stream()
                    .map(host -> "'" + scheme + "://" + host + ":" + port + "/" + token + "'")
                    .collect(Collectors.toList());
        }

        private Connection openConnection() throws SQLException {
            String url = "jdbc:postgresql://" + table.getHost() + ":" + table.getPort()
                    + "/" + table.getDatabase();
            Properties props = new Properties();
            props.setProperty("user", table.getConnectInfo().get(GreenplumConnectorConstants.USER));
            props.setProperty("password", table.getConnectInfo().get(GreenplumConnectorConstants.PASSWORD));
            props.setProperty("connectTimeout", "10");
            props.setProperty("options", "-c TimeZone=UTC");
            return DriverManager.getConnection(url, props);
        }

        private void rollbackQuietly() {
            Connection conn = connection;
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    LOG.warn("rollback failed for greenplum load {}", token, e);
                }
            }
        }

        private void closeQuietly() {
            Connection conn = connection;
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOG.warn("close failed for greenplum load {}", token, e);
                }
                connection = null;
            }
        }

        private static String quote(String identifier) {
            return "\"" + identifier + "\"";
        }

        private static String escape(String value) {
            return value.replace("\\", "\\\\").replace("'", "''");
        }
    }
}
