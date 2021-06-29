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
package io.trino.plugin.jdbc.mapping;

import io.trino.plugin.jdbc.JdbcIdentity;

import java.sql.Connection;

public interface IdentifierMapping
{
    String fromRemoteSchemaName(String remoteSchemaName);

    String fromRemoteTableName(String remoteSchemaName, String remoteTableName);

    String fromRemoteColumnName(String remoteColumnName);

    String toRemoteSchemaName(JdbcIdentity identity, Connection connection, String schemaName);

    String toRemoteTableName(JdbcIdentity identity, Connection connection, String remoteSchema, String tableName);

    String toRemoteColumnName(Connection connection, String columnName);
}
