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
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TGreenplumTableSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Sink for INSERT INTO a Greenplum (Arenadata DB) external table.
 * <p>
 * The BE side registers a gpfdist session under {@code sessionToken} and
 * serves the query's result rows to Greenplum segments, which pull them
 * through a READABLE EXTERNAL TABLE created and driven by the FE-side
 * {@code GreenplumLoadOrchestrator} concurrently with query execution.
 * The sink itself carries no credentials: the BE never connects to Greenplum.
 */
public class GreenplumTableSink extends DataSink {

    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final String sessionToken;
    private final String columnSeparator;
    private final String nullMarker;
    private final String transportScheme;

    public GreenplumTableSink(GreenplumTable table, String sessionToken,
                              String columnSeparator, String nullMarker, String transportScheme) {
        this.schemaName = table.getCatalogDBName();
        this.tableName = table.getCatalogTableName();
        this.columnNames = new ArrayList<>();
        for (Column column : table.getFullSchema()) {
            this.columnNames.add(column.getName());
        }
        this.sessionToken = sessionToken;
        this.columnSeparator = columnSeparator;
        this.nullMarker = nullMarker;
        this.transportScheme = transportScheme;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("GREENPLUM TABLE SINK\n");
        strBuilder.append(prefix).append("  TABLE: ").append(schemaName).append(".").append(tableName).append("\n");
        strBuilder.append(prefix).append("  TRANSPORT: ").append(transportScheme).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.GREENPLUM_TABLE_SINK);
        TGreenplumTableSink tSink = new TGreenplumTableSink();
        tSink.setSchema_name(schemaName);
        tSink.setTable_name(tableName);
        tSink.setColumn_names(columnNames);
        tSink.setSession_token(sessionToken);
        tSink.setColumn_separator(columnSeparator);
        tSink.setNull_marker(nullMarker);
        tSink.setTransport_scheme(transportScheme);
        tDataSink.setGreenplum_table_sink(tSink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
