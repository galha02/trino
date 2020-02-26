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
package io.prestosql.plugin.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.NotFoundException;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult.Assignment;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.expression.ConnectorExpression;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    static final int NUMERIC_DATA_TYPE_PRECISION = 38;
    static final int NUMERIC_DATA_TYPE_SCALE = 9;
    private static final Logger log = Logger.get(BigQueryMetadata.class);
    private BigQuery bigQuery;
    private String projectId;

    @Inject
    public BigQueryMetadata(BigQuery bigQuery, BigQueryConfig config)
    {
        this.bigQuery = bigQuery;
        this.projectId = config.getProjectId().orElse(bigQuery.getOptions().getProjectId());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("listSchemaNames(session=%s)", session);
        return Streams.stream(bigQuery.listDatasets(projectId).iterateAll())
                .map(dataset -> dataset.getDatasetId().getDataset())
                .collect(toImmutableList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        log.debug("listTables(session=%s, schemaName=%s)", session, schemaName);
        Set<String> schemaNames = schemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String datasetId : schemaNames) {
            for (Table table : bigQuery.listTables(DatasetId.of(projectId, datasetId)).iterateAll()) {
                tableNames.add(new SchemaTableName(datasetId, table.getTableId().getTable()));
            }
        }
        return tableNames.build();
    }

    <T> ImmutableList<T> collectAll(Page<T> page)
    {
        return ImmutableList.copyOf(page.iterateAll());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        log.debug("getTableHandle(session=%s, tableName=%s)", session, tableName);
        Table table = bigQuery.getTable(TableId.of(projectId, tableName.getSchemaName(), tableName.getTableName()));
        if (table == null) {
            throw new TableNotFoundException(tableName);
        }
        return BigQueryTableHandle.from(table);
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        ConnectorTableHandle table = getTableHandle(session, tableName);
        return getTableMetadata(session, table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        Table table = bigQuery.getTable(((BigQueryTableHandle) tableHandle).getTableId());
        SchemaTableName schemaTableName = new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable());
        List<ColumnMetadata> columns = table.getDefinition().getSchema().getFields().stream()
                .map(Conversions::toColumnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getColumnHandles(session=%s, tableHandle=%s)", session, tableHandle);
        Table table = bigQuery.getTable(((BigQueryTableHandle) tableHandle).getTableId());
        return table.getDefinition().getSchema().getFields().stream()
                .collect(toMap(Field::getName, Conversions::toColumnHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        log.debug("getColumnMetadata(session=%s, tableHandle=%s, columnHandle=%s)", session, columnHandle, columnHandle);
        return ((BigQueryColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        log.debug("listTableColumns(session=%s, prefix=%s)", session, prefix);
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (!prefix.getTable().isPresent()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        log.debug("getTableProperties(session=%s, prefix=%s)", session, table);
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        log.debug("applyLimit(session=%s, handle=%s, limit=%s)", session, handle, limit);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getLimit().isPresent() && bigQueryTableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        bigQueryTableHandle = bigQueryTableHandle.withLimit(limit);

        return Optional.of(new LimitApplicationResult<>(bigQueryTableHandle, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        log.debug("applyProjection(session=%s, handle=%s, projections=%s, assignments=%s)",
                session, handle, projections, assignments);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getProjectedColumns().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            projectedColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((BigQueryColumnHandle) column).getPrestoType()));
        });

        bigQueryTableHandle = bigQueryTableHandle.withProjectedColumns(projectedColumns.build());

        return Optional.of(new ProjectionApplicationResult<>(bigQueryTableHandle, projections, assignmentList.build()));
    }
}
