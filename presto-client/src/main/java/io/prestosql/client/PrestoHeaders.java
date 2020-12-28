/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.client;

public final class PrestoHeaders
{
    public static final String PRESTO_USER = "X-Presto-User";
    public static final String PRESTO_SOURCE = "X-Presto-Source";
    public static final String PRESTO_CATALOG = "X-Presto-Catalog";
    public static final String PRESTO_SCHEMA = "X-Presto-Schema";
    public static final String PRESTO_PATH = "X-Presto-Path";
    public static final String PRESTO_TIME_ZONE = "X-Presto-Time-Zone";
    public static final String PRESTO_LANGUAGE = "X-Presto-Language";
    public static final String PRESTO_TRACE_TOKEN = "X-Presto-Trace-Token";
    public static final String PRESTO_SESSION = "X-Presto-Session";
    public static final String PRESTO_SET_CATALOG = "X-Presto-Set-Catalog";
    public static final String PRESTO_SET_SCHEMA = "X-Presto-Set-Schema";
    public static final String PRESTO_SET_PATH = "X-Presto-Set-Path";
    public static final String PRESTO_SET_SESSION = "X-Presto-Set-Session";
    public static final String PRESTO_CLEAR_SESSION = "X-Presto-Clear-Session";
    public static final String PRESTO_SET_ROLE = "X-Presto-Set-Role";
    public static final String PRESTO_ROLE = "X-Presto-Role";
    public static final String PRESTO_PREPARED_STATEMENT = "X-Presto-Prepared-Statement";
    public static final String PRESTO_ADDED_PREPARE = "X-Presto-Added-Prepare";
    public static final String PRESTO_DEALLOCATED_PREPARE = "X-Presto-Deallocated-Prepare";
    public static final String PRESTO_TRANSACTION_ID = "X-Presto-Transaction-Id";
    public static final String PRESTO_STARTED_TRANSACTION_ID = "X-Presto-Started-Transaction-Id";
    public static final String PRESTO_CLEAR_TRANSACTION_ID = "X-Presto-Clear-Transaction-Id";
    public static final String PRESTO_CLIENT_INFO = "X-Presto-Client-Info";
    public static final String PRESTO_CLIENT_TAGS = "X-Presto-Client-Tags";
    public static final String PRESTO_CLIENT_CAPABILITIES = "X-Presto-Client-Capabilities";
    public static final String PRESTO_RESOURCE_ESTIMATE = "X-Presto-Resource-Estimate";
    public static final String PRESTO_EXTRA_CREDENTIAL = "X-Presto-Extra-Credential";

    private PrestoHeaders() {}
}
