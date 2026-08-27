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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.GreenplumScanNode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives the Greenplum side of the gpfdist READ path: pushing rows from a
 * Greenplum source into a StarRocks BE that runs the scan.
 * <p>
 * Concurrently with query execution, on its own thread, one GP transaction:
 * <pre>
 *   BEGIN;
 *   CREATE WRITABLE EXTERNAL TABLE ext_rd_&lt;token&gt; (&lt;projected cols&gt;)
 *     LOCATION('gpfdist://&lt;scanBE&gt;:&lt;port&gt;/&lt;token&gt;') FORMAT 'TEXT'(...)
 *     DISTRIBUTED RANDOMLY;   -- avoids a redistribute motion
 *   INSERT INTO ext_rd_&lt;token&gt; &lt;the scan's SELECT&gt;;  -- segments push to the BE
 *   DROP EXTERNAL TABLE ext_rd_&lt;token&gt;;
 *   COMMIT;
 * </pre>
 * The BE scan registers a PushSession under the token before the FE issues
 * this statement, so there is no race. Transactional DDL means no orphans.
 */
public class GreenplumReadOrchestrator {

    private static final Logger LOG = LogManager.getLogger(GreenplumReadOrchestrator.class);

    private static final ExecutorService EXECUTOR =
            ThreadPoolManager.newDaemonCacheThreadPool(64, "greenplum-read", true);

    private GreenplumReadOrchestrator() {
    }

    /** Stable per-query token prefix; scan nodes append their node id. */
    public static String sessionToken(TUniqueId executionId) {
        return DebugUtil.printId(executionId).replace("-", "_");
    }

    /** Launch the GP-side push for one greenplum scan running on `scanHost`. */
    public static Handle start(GreenplumScanNode scanNode, String scanHost, int timeoutSec) {
        Handle handle = new Handle(scanNode, scanHost, timeoutSec);
        handle.submit();
        return handle;
    }

    public static class Handle {
        private final GreenplumScanNode scanNode;
        private final GreenplumTable table;
        private final String token;
        private final String scanHost;
        private final int timeoutSec;
        private final AtomicBoolean aborted = new AtomicBoolean(false);

        private volatile Connection connection;
        private volatile Statement runningStatement;
        private volatile Future<Void> future;

        private Handle(GreenplumScanNode scanNode, String scanHost, int timeoutSec) {
            this.scanNode = scanNode;
            this.table = scanNode.getGreenplumTable();
            this.token = scanNode.getSessionToken();
            this.scanHost = scanHost;
            this.timeoutSec = timeoutSec;
        }

        private void submit() {
            future = EXECUTOR.submit(() -> {
                try {
                    runPushTransaction();
                } catch (Throwable t) {
                    // Self-clean so an un-joined failed load never leaks a GP
                    // connection: roll back here; join()/abort() are then no-ops.
                    rollbackQuietly();
                    closeQuietly();
                    throw t;
                }
                return null;
            });
        }

        /** Wait for the GP push to finish (segments have sent all rows + DONE). */
        public void join() {
            try {
                future.get(timeoutSec, TimeUnit.SECONDS);
                connection.commit();
            } catch (Exception e) {
                abort("greenplum read push failed: " + e.getMessage());
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new StarRocksConnectorException("greenplum read failed: " + cause.getMessage(), cause);
            } finally {
                closeQuietly();
            }
        }

        public void abort(String reason) {
            if (!aborted.compareAndSet(false, true)) {
                return;
            }
            LOG.warn("aborting greenplum read {}: {}", token, reason);
            Statement stmt = runningStatement;
            if (stmt != null) {
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    LOG.warn("cancel failed for greenplum read {}", token, e);
                }
            }
            if (future != null) {
                future.cancel(true);
            }
            rollbackQuietly();
            closeQuietly();
        }

        private void runPushTransaction() throws SQLException {
            Connection conn = openConnection();
            this.connection = conn;
            conn.setAutoCommit(false);
            String extTable = quote(table.getCatalogDBName()) + "." + quote("ext_rd_" + token);
            try (Statement stmt = conn.createStatement()) {
                this.runningStatement = stmt;
                // Pin the GUCs that govern how the segments render the pushed
                // TEXT rows, so the BE decoder sees a stable, loss-free dialect
                // regardless of the target cluster's defaults:
                //   DateStyle=ISO,YMD  -> unambiguous "YYYY-MM-DD[ HH:MM:SS]"
                //                         (a non-ISO default would emit e.g.
                //                         "01-02-2024", which the BE misparses)
                //   extra_float_digits=3 -> shortest round-trippable float/double
                //                         (GP6's PG heritage defaults to 0, which
                //                         truncates real/double precision on output)
                stmt.execute("SET DateStyle TO 'ISO, YMD'");
                stmt.execute("SET extra_float_digits TO 3");
                // A timestamptz source column cast into the writable ext table's
                // `timestamp` column is rendered in the session time zone; pin
                // UTC so the wall-clock the BE decodes is deterministic and
                // matches the connector's UTC contract (same rule the libpq scan
                // path applies).
                stmt.execute("SET TimeZone TO 'UTC'");
                stmt.execute(buildCreateWritableExternalTableSql(extTable));
                // Blocks while segments push rows to the BE gpfdist endpoint.
                stmt.execute("INSERT INTO " + extTable + " " + scanNode.getReadSelectSql());
                stmt.execute("DROP EXTERNAL TABLE " + extTable);
            } finally {
                this.runningStatement = null;
            }
        }

        String buildCreateWritableExternalTableSql(String extTable) {
            List<String> colDefs = new ArrayList<>();
            for (Column col : scanNode.getProjectedColumns()) {
                colDefs.add(quote(col.getName()) + " " + greenplumType(col.getType()));
            }
            String location = "'" + table.getTransportScheme() + "://" + scanHost + ":"
                    + table.getGpfdistPort() + "/" + token + "'";
            return "CREATE WRITABLE EXTERNAL TABLE " + extTable + " (" + Joiner.on(", ").join(colDefs) + ") "
                    + "LOCATION(" + location + ") "
                    + "FORMAT 'TEXT' (DELIMITER E'" + escape(table.getColumnSeparator())
                    + "' NULL E'" + escape(table.getNullMarker()) + "') "
                    + "DISTRIBUTED RANDOMLY";
        }

        // StarRocks type -> Greenplum column type for the external-table DDL.
        // The external table is only a transient sink for TEXT bytes, so the
        // widest compatible GP type is safe; the BE decoder enforces the real
        // target types.
        static String greenplumType(Type type) {
            PrimitiveType pt = type.getPrimitiveType();
            switch (pt) {
                case BOOLEAN:
                    return "boolean";
                case TINYINT:
                case SMALLINT:
                    return "smallint";
                case INT:
                    return "integer";
                case BIGINT:
                case LARGEINT:
                    return "bigint";
                case FLOAT:
                    return "real";
                case DOUBLE:
                    return "double precision";
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    return "numeric";
                case DATE:
                    return "date";
                case DATETIME:
                    return "timestamp";
                default:
                    return "text";
            }
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
                    LOG.warn("rollback failed for greenplum read {}", token, e);
                }
            }
        }

        private void closeQuietly() {
            Connection conn = connection;
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOG.warn("close failed for greenplum read {}", token, e);
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
