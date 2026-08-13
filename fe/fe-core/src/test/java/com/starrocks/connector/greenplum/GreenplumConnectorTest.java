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

import com.starrocks.catalog.GreenplumTable;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GreenplumConnectorTest {

    private Map<String, String> properties;

    @BeforeEach
    public void setUp() {
        FeConstants.runningUnitTest = true;
        properties = new HashMap<>();
        properties.put(GreenplumConnectorConstants.HOST, "127.0.0.1");
        properties.put(GreenplumConnectorConstants.DATABASE, "gpdb");
        properties.put(GreenplumConnectorConstants.USER, "gpadmin");
        properties.put(GreenplumConnectorConstants.PASSWORD, "123456");
    }

    @Test
    public void testValidPropertiesConstructsFine() {
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertDoesNotThrow(() -> new GreenplumConnector(context));
    }

    @Test
    public void testMissingHostThrows() {
        properties.remove(GreenplumConnectorConstants.HOST);
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> new GreenplumConnector(context));
    }

    @Test
    public void testMissingDatabaseThrows() {
        properties.remove(GreenplumConnectorConstants.DATABASE);
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> new GreenplumConnector(context));
    }

    @Test
    public void testMissingUserThrows() {
        properties.remove(GreenplumConnectorConstants.USER);
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> new GreenplumConnector(context));
    }

    @Test
    public void testMissingPasswordThrows() {
        properties.remove(GreenplumConnectorConstants.PASSWORD);
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> new GreenplumConnector(context));
    }

    @Test
    public void testPortDefaultsWhenOmitted() {
        Assertions.assertNull(properties.get(GreenplumConnectorConstants.PORT));
        ConnectorContext context = new ConnectorContext("greenplum0", "greenplum", properties);
        Assertions.assertDoesNotThrow(() -> new GreenplumConnector(context));
        Assertions.assertEquals(GreenplumConnectorConstants.DEFAULT_PORT,
                properties.get(GreenplumConnectorConstants.PORT));
    }

    @Test
    public void testGreenplumTablePortDefaultsWhenOmitted() {
        Assertions.assertNull(properties.get(GreenplumConnectorConstants.PORT));
        GreenplumTable table = new GreenplumTable(1L, "tbl", Collections.emptyList(),
                "gp_schema", "greenplum0", properties);
        Assertions.assertEquals(GreenplumConnectorConstants.DEFAULT_PORT, table.getPort());
    }
}
