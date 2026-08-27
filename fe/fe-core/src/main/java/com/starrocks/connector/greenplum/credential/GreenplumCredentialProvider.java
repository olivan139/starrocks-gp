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

/**
 * Resolves the {@link GreenplumCredential} used to open connections to a Greenplum catalog.
 * <p>
 * The current contract is intentionally minimal: a single, catalog-wide credential. A future
 * revision may add an overload such as {@code get(String principal)} to support per-principal
 * credentials (e.g. secman-backed impersonation), at which point {@link #get()} can delegate to
 * it using the current session's identity.
 */
public interface GreenplumCredentialProvider {

    /**
     * @return the credential to use for the current connection.
     */
    GreenplumCredential get();

    /**
     * Invalidate a stale credential, e.g. after an authentication failure, so the next call to
     * {@link #get()} can refresh it. Providers backed by a static configuration can ignore this.
     */
    default void invalidate(GreenplumCredential stale) {
    }
}
