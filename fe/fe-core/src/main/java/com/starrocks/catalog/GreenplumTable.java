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

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.greenplum.GreenplumConnectorConstants;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TGreenplumTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Catalog-mode-only external table backed by a Greenplum (or PostgreSQL-compatible) remote
 * table. Unlike {@link JDBCTable} there is no legacy resource-based creation path here.
 */
public class GreenplumTable extends Table {

    @SerializedName(value = "tn")
    private String greenplumTableName;

    // Not persisted: rebuilt from the owning catalog's properties every time the table is
    // fetched from the connector.
    private Map<String, String> connectInfo;
    private String catalogName;
    private String dbName;

    public GreenplumTable() {
        super(TableType.GREENPLUM);
    }

    public GreenplumTable(long id, String name, List<Column> schema, String dbName,
                          String catalogName, Map<String, String> properties) {
        super(id, name, TableType.GREENPLUM, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.greenplumTableName = name;
        validate(properties);
    }

    private void validate(Map<String, String> properties) {
        if (properties == null) {
            throw new StarRocksConnectorException("Please set properties of greenplum table, they are: "
                    + GreenplumConnectorConstants.HOST + ", " + GreenplumConnectorConstants.DATABASE + ", "
                    + GreenplumConnectorConstants.USER + ", " + GreenplumConnectorConstants.PASSWORD);
        }

        if (properties.get(GreenplumConnectorConstants.HOST) == null ||
                properties.get(GreenplumConnectorConstants.DATABASE) == null ||
                properties.get(GreenplumConnectorConstants.USER) == null ||
                properties.get(GreenplumConnectorConstants.PASSWORD) == null) {
            throw new StarRocksConnectorException("all catalog properties must be set");
        }

        Map<String, String> newConnectInfo = new HashMap<>(properties);
        if (Strings.isNullOrEmpty(newConnectInfo.get(GreenplumConnectorConstants.PORT))) {
            newConnectInfo.put(GreenplumConnectorConstants.PORT, GreenplumConnectorConstants.DEFAULT_PORT);
        }
        this.connectInfo = newConnectInfo;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public String getCatalogTableName() {
        return greenplumTableName;
    }

    public Map<String, String> getConnectInfo() {
        if (connectInfo == null) {
            this.connectInfo = new HashMap<>();
        }
        return connectInfo;
    }

    public String getHost() {
        return getConnectInfo().get(GreenplumConnectorConstants.HOST);
    }

    public String getPort() {
        return getConnectInfo().get(GreenplumConnectorConstants.PORT);
    }

    public String getDatabase() {
        return getConnectInfo().get(GreenplumConnectorConstants.DATABASE);
    }

    public String getTransport() {
        return getConnectInfo().getOrDefault(GreenplumConnectorConstants.TRANSPORT,
                GreenplumConnectorConstants.TRANSPORT_COPY);
    }

    // TODO, identify the remote table that created after deleted
    @Override
    public String getUUID() {
        if (!Strings.isNullOrEmpty(catalogName)) {
            return String.join(".", catalogName, dbName, name);
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TGreenplumTable tGreenplumTable = new TGreenplumTable();
        tGreenplumTable.setHost(connectInfo.get(GreenplumConnectorConstants.HOST));
        tGreenplumTable.setPort(connectInfo.get(GreenplumConnectorConstants.PORT));
        tGreenplumTable.setDatabase(connectInfo.get(GreenplumConnectorConstants.DATABASE));
        tGreenplumTable.setUser(connectInfo.get(GreenplumConnectorConstants.USER));
        tGreenplumTable.setPasswd(connectInfo.get(GreenplumConnectorConstants.PASSWORD));

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.GREENPLUM_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setGreenplumTable(tGreenplumTable);
        return tTableDescriptor;
    }

    @Override
    public Set<TableOperation> getSupportedOperations() {
        return Sets.newHashSet(TableOperation.READ);
    }
}
