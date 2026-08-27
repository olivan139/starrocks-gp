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

/**
 * Property key/value constants shared across the Greenplum connector.
 */
public final class GreenplumConnectorConstants {

    public static final String HOST = "greenplum.host";
    public static final String PORT = "greenplum.port";
    public static final String DATABASE = "greenplum.database";
    public static final String USER = "greenplum.user";
    public static final String PASSWORD = "greenplum.password";
    public static final String CREDENTIAL_PROVIDER = "greenplum.credential.provider";
    public static final String CREDENTIAL_REF = "greenplum.credential.ref";
    public static final String TRANSPORT = "greenplum.transport";
    // gpfdist listener port on every BE (single shared value; per-BE ports may come later)
    public static final String GPFDIST_PORT = "greenplum.gpfdist.port";
    // "gpfdist" (plain, dev) or "gpfdists" (mutual TLS, production)
    public static final String TRANSPORT_SCHEME = "greenplum.gpfdist.scheme";
    // row format used both in the external-table DDL and by the BE sink encoder
    public static final String COLUMN_SEPARATOR = "greenplum.format.column_separator";
    public static final String NULL_MARKER = "greenplum.format.null_marker";

    public static final String DEFAULT_PORT = "5432";
    public static final String TRANSPORT_COPY = "copy";
    public static final String DEFAULT_GPFDIST_PORT = "8907";
    public static final String DEFAULT_TRANSPORT_SCHEME = "gpfdist";
    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    public static final String DEFAULT_NULL_MARKER = "\\N";

    private GreenplumConnectorConstants() {
    }
}
