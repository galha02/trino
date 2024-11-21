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
package io.trino.plugin.loki;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.github.jeschkies.loki.client.LokiClient;
import io.github.jeschkies.loki.client.LokiClientException;
import io.github.jeschkies.loki.client.model.Data;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.loki.LokiTableFunction.SCHEMA_NAME;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class LokiMetadata
        implements ConnectorMetadata
{
    private static final String TABLE_NAME = "default";

    private final Type labelsMapType;

    private final LokiClient lokiClient;

    @Inject
    public LokiMetadata(LokiClient lokiClient, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.lokiClient = requireNonNull(lokiClient, "lokiClient is null");
        labelsMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof LokiTableFunction.QueryHandle queryHandle)) {
            return Optional.empty();
        }

        LokiTableHandle tableHandle = queryHandle.getTableHandle();
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, tableHandle.columnHandles()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LokiTableHandle lokiTableHandle = (LokiTableHandle) table;

        List<ColumnMetadata> columns = lokiTableHandle.columnHandles()
                .stream()
                .map(columnHandle -> ((LokiColumnHandle) columnHandle).columnMetadata())
                .collect(toImmutableList());

        return new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), columns);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        throw new TrinoException(LokiErrorCode.LOKI_TABLE_ERROR, "Loki connector does not support querying tables directly. Use the TABLE function instead.");
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        LokiColumnHandle lokiColumnHandler = (LokiColumnHandle) columnHandle;
        return lokiColumnHandler.columnMetadata();
    }

    public List<ColumnHandle> getColumnHandles(String query)
    {
        ImmutableList.Builder<ColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(3);
        columnsBuilder.add(new LokiColumnHandle("labels", this.labelsMapType, 0));
        columnsBuilder.add(new LokiColumnHandle("timestamp", TIMESTAMP_TZ_NANOS, 1));

        try {
            if (lokiClient.getExpectedResultType(query) == Data.ResultType.Matrix) {
                columnsBuilder.add(new LokiColumnHandle("value", DoubleType.DOUBLE, 2));
            }
            else {
                columnsBuilder.add(new LokiColumnHandle("value", VarcharType.VARCHAR, 2));
            }
        }
        catch (LokiClientException e) {
            throw new TrinoException(LokiErrorCode.LOKI_CLIENT_ERROR, e);
        }
        return columnsBuilder.build();
    }
}
