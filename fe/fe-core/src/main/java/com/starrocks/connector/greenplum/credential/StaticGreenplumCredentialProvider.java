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

package com.starrocks.connector.greenplum.credential;

import com.google.common.base.Strings;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.greenplum.GreenplumConnectorConstants;

import java.util.Map;

/**
 * A {@link GreenplumCredentialProvider} backed by a static user/password pair read once from the
 * catalog properties at construction time.
 */
public class StaticGreenplumCredentialProvider implements GreenplumCredentialProvider {

    private final GreenplumCredential credential;

    public StaticGreenplumCredentialProvider(Map<String, String> properties) {
        String user = properties.get(GreenplumConnectorConstants.USER);
        if (Strings.isNullOrEmpty(user)) {
            throw new StarRocksConnectorException(
                    "Missing " + GreenplumConnectorConstants.USER + " in properties");
        }
        String password = properties.get(GreenplumConnectorConstants.PASSWORD);
        this.credential = new GreenplumCredential(user, password);
    }

    @Override
    public GreenplumCredential get() {
        return credential;
    }
}
