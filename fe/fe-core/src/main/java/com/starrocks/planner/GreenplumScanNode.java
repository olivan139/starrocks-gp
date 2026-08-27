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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.greenplum.GreenplumReadOrchestrator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TGreenplumScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;

/**
 * Full scan on a Greenplum (Arenadata DB) external table.
 * <p>
 * Generates a PostgreSQL-dialect SELECT (double-quoted identifiers,
 * schema-qualified table name). The complete statement is shipped to the BE in
 * {@code TGreenplumScanNode.sql} so the native (libpq) reader can execute it
 * verbatim, e.g. wrapped in {@code COPY (<sql>) TO STDOUT}.
 * <p>
 * The scan is single-node in this milestone: {@link #getScanRangeLocations}
 * returns null and the fragment is UNPARTITIONED, mirroring JDBCScanNode.
 */
public class GreenplumScanNode extends ScanNode {

    private static final String IDENTIFIER = "\"";

    private final List<String> columns = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private final GreenplumTable table;
    private final String tableName;
    private final String sessionToken;

    public GreenplumScanNode(PlanNodeId id, TupleDescriptor desc, GreenplumTable tbl) {
        super(id, desc, "SCAN GREENPLUM");
        table = tbl;
        tableName = qualifiedTableName(tbl);
        ConnectContext ctx = ConnectContext.get();
        String qid = ctx != null && ctx.getExecutionId() != null
                ? GreenplumReadOrchestrator.sessionToken(ctx.getExecutionId()) : "noctx";
        this.sessionToken = qid + "_n" + id.asInt();
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public GreenplumTable getGreenplumTable() {
        return table;
    }

    // Column definitions for the writable external table the read orchestrator
    // creates on Greenplum: the PROJECTED, materialized scan columns, in the
    // order the BE decoder expects (== the tuple's materialized slots).
    public List<Column> getProjectedColumns() {
        List<Column> cols = new ArrayList<>();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.isMaterialized() && slot.getColumn() != null) {
                cols.add(slot.getColumn());
            }
        }
        return cols;
    }

    // The SELECT that reads the projected/filtered rows from the GP source;
    // wrapped by the orchestrator as INSERT INTO <ext> <this sql>.
    public String getReadSelectSql() {
        return buildQuerySql();
    }

    private static String qualifiedTableName(GreenplumTable tbl) {
        String schema = tbl.getCatalogDBName();
        String name = tbl.getCatalogTableName();
        if (schema == null || schema.isEmpty()) {
            return quote(name);
        }
        return quote(schema) + "." + quote(name);
    }

    private static String quote(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return "";
        }
        if (identifier.length() > 2 && identifier.startsWith(IDENTIFIER) && identifier.endsWith(IDENTIFIER)) {
            return identifier;
        }
        return IDENTIFIER + identifier + IDENTIFIER;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalizeStats() throws StarRocksException {
        createColumns();
        createFilters();
        computeStats();
    }

    public void computeColumnsAndFilters() {
        createColumns();
        createFilters();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(tableName).append("\n");
        output.append(prefix).append("QUERY: ").append(buildQuerySql()).append("\n");
        return output.toString();
    }

    private String buildQuerySql() {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(Joiner.on(", ").join(columns));
        sql.append(" FROM ").append(tableName);
        if (!filters.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(Joiner.on(") AND (").join(filters));
            sql.append(")");
        }
        if (limit != -1) {
            sql.append(" LIMIT ").append(limit);
        }
        return sql.toString();
    }

    private void createColumns() {
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            columns.add(quote(slot.getColumn().getName()));
        }
        // this happens when count(*)
        if (columns.isEmpty()) {
            columns.add("*");
        }
    }

    private void createFilters() {
        if (conjuncts.isEmpty()) {
            return;
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        ExprUtils.collectList(conjuncts, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(quote(tmpRef.getLabel()));
            sMap.put(slotRef, tmpRef);
        }

        ArrayList<Expr> gpConjuncts = ExprUtils.cloneList(conjuncts, sMap);
        // Filters instead of conjuncts are used in BE to filter rows: the types of
        // conjuncts' children would be unmatched after the cast removal done by
        // PushDownPredicateToExternalTableScanRule (same reasoning as JDBCScanNode).
        conjuncts.clear();
        for (Expr p : gpConjuncts) {
            p = ExprUtils.replaceLargeStringLiteral(p);
            filters.add(AstToStringBuilder.toString(p));
        }
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.GREENPLUM_SCAN_NODE;
        msg.greenplum_scan_node = new TGreenplumScanNode();
        msg.greenplum_scan_node.setTuple_id(desc.getId().asInt());
        msg.greenplum_scan_node.setTable_name(tableName);
        msg.greenplum_scan_node.setColumns(columns);
        msg.greenplum_scan_node.setFilters(filters);
        msg.greenplum_scan_node.setLimit(limit);
        msg.greenplum_scan_node.setSql(buildQuerySql());
        msg.greenplum_scan_node.setTransport("gpfdist");
        msg.greenplum_scan_node.setSession_token(sessionToken);
        msg.greenplum_scan_node.setColumn_separator(table.getColumnSeparator());
        msg.greenplum_scan_node.setNull_marker(table.getNullMarker());
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return null;
    }

    @Override
    public void computeStats() {
        super.computeStats();
    }
}
