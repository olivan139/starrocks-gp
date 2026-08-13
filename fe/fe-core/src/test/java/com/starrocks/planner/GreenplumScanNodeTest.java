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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.GreenplumTable;
import com.starrocks.connector.greenplum.GreenplumConnectorConstants;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class GreenplumScanNodeTest {

    private Map<String, String> gpProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(GreenplumConnectorConstants.HOST, "127.0.0.1");
        properties.put(GreenplumConnectorConstants.PORT, "5432");
        properties.put(GreenplumConnectorConstants.DATABASE, "gpdb");
        properties.put(GreenplumConnectorConstants.USER, "gpadmin");
        properties.put(GreenplumConnectorConstants.PASSWORD, "123456");
        return properties;
    }

    private SlotDescriptor createSlotDescriptor(int slotId, Column column) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), column.getName(), column.getType(), true);
        slot.setColumn(column);
        slot.setIsMaterialized(true);
        return slot;
    }

    private GreenplumScanNode createScanNode(List<Column> columns, List<SlotDescriptor> slots) {
        GreenplumTable table = new GreenplumTable(1, "orders", columns, "public", "greenplum0", gpProperties());
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(table);
        for (SlotDescriptor slot : slots) {
            tupleDesc.addSlot(slot);
        }
        return new GreenplumScanNode(new PlanNodeId(1), tupleDesc, table);
    }

    @Test
    public void testExplainStringContainsQualifiedTableAndColumns() {
        Column idColumn = new Column("id", IntegerType.INT);
        Column nameColumn = new Column("name", VarcharType.VARCHAR);
        SlotDescriptor idSlot = createSlotDescriptor(1, idColumn);
        SlotDescriptor nameSlot = createSlotDescriptor(2, nameColumn);
        GreenplumScanNode scanNode = createScanNode(Lists.newArrayList(idColumn, nameColumn),
                Lists.newArrayList(idSlot, nameSlot));
        scanNode.computeColumnsAndFilters();

        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("\"public\".\"orders\""), nodeString);
        Assertions.assertTrue(nodeString.contains("\"id\", \"name\""), nodeString);
    }

    @Test
    public void testExplainStringForCountStarUsesSelectStar() {
        Column idColumn = new Column("id", IntegerType.INT);
        GreenplumScanNode scanNode = createScanNode(Lists.newArrayList(idColumn), Lists.newArrayList());
        scanNode.computeColumnsAndFilters();

        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("SELECT *"), nodeString);
    }

    @Test
    public void testToThrift() {
        Column idColumn = new Column("id", IntegerType.INT);
        SlotDescriptor idSlot = createSlotDescriptor(1, idColumn);
        GreenplumScanNode scanNode = createScanNode(Lists.newArrayList(idColumn), Lists.newArrayList(idSlot));
        scanNode.computeColumnsAndFilters();

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);

        Assertions.assertEquals(TPlanNodeType.GREENPLUM_SCAN_NODE, thriftNode.node_type);
        Assertions.assertTrue(thriftNode.isSetGreenplum_scan_node());
        String sql = thriftNode.getGreenplum_scan_node().getSql();
        Assertions.assertTrue(sql.startsWith("SELECT \"id\""), sql);
        Assertions.assertTrue(sql.contains("FROM \"public\".\"orders\""), sql);
    }

    @Test
    public void testLimitPropagatedToSql() {
        Column idColumn = new Column("id", IntegerType.INT);
        SlotDescriptor idSlot = createSlotDescriptor(1, idColumn);
        GreenplumScanNode scanNode = createScanNode(Lists.newArrayList(idColumn), Lists.newArrayList(idSlot));
        scanNode.setLimit(7);
        scanNode.computeColumnsAndFilters();

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);
        String sql = thriftNode.getGreenplum_scan_node().getSql();
        Assertions.assertTrue(sql.contains("LIMIT 7"), sql);

        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("LIMIT 7"), nodeString);
    }

    @Test
    public void testConjunctsProduceFiltersAndWhereClause() {
        Column dateColumn = new Column("d", DateType.DATE);
        SlotDescriptor dateSlot = createSlotDescriptor(1, dateColumn);
        GreenplumScanNode scanNode = createScanNode(Lists.newArrayList(dateColumn), Lists.newArrayList(dateSlot));
        Expr predicate = new BinaryPredicate(BinaryType.EQ,
                new SlotRef("d", dateSlot), StringLiteral.create("2026-01-01"));
        scanNode.getConjuncts().add(predicate);
        scanNode.computeColumnsAndFilters();

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);
        Assertions.assertFalse(thriftNode.getGreenplum_scan_node().getFilters().isEmpty());

        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("WHERE"), nodeString);
    }
}
