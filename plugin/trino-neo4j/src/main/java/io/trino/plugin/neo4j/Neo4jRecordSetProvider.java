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
package io.trino.plugin.neo4j;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class Neo4jRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Neo4jClient client;
    private final Neo4jTypeManager typeManager;

    @Inject
    public Neo4jRecordSetProvider(
            Neo4jClient client,
            Neo4jTypeManager typeManager)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        Neo4jTableHandle tableHandle = (Neo4jTableHandle) table;

        List<Neo4jColumnHandle> columnHandles = columns.stream()
                .map(c -> (Neo4jColumnHandle) c)
                .collect(toImmutableList());

        String cypher = requireNonNull(client, "client is null").toCypher(tableHandle, columnHandles);

        return new Neo4jRecordSet(
                this.client,
                this.typeManager,
                tableHandle.getRelationHandle().getDatabaseName(),
                cypher,
                columnHandles);
    }
}
