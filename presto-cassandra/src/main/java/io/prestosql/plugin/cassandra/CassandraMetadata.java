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
package io.prestosql.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.NotFoundException;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.prestosql.plugin.cassandra.CassandraType.toCassandraType;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.ID_COLUMN_NAME;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.cqlNameToSqlName;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.validColumnName;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    private final CassandraSession cassandraSession;
    private final CassandraPartitionManager partitionManager;
    private final boolean allowDropTable;

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;

    @Inject
    public CassandraMetadata(
            CassandraSession cassandraSession,
            CassandraPartitionManager partitionManager,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            CassandraClientConfig config)
    {
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.allowDropTable = requireNonNull(config, "config is null").getAllowDropTable();
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return cassandraSession.getCaseSensitiveSchemaNames().stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public CassandraTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            return cassandraSession.getTable(tableName).getTableHandle();
        }
        catch (TableNotFoundException | SchemaNotFoundException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((CassandraTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return getTableMetadata(getTableName(tableHandle));
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        CassandraTable table = cassandraSession.getTable(tableName);
        List<ColumnMetadata> columns = table.getColumns().stream()
                .map(CassandraColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName1)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        List<String> schemaNames = listSchemas(session, schemaName1);
        for (String schemaName : schemaNames) {
            try {
                for (String tableName : cassandraSession.getCaseSensitiveTableNames(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName.map(ImmutableList::of)
                .orElseGet(() -> (ImmutableList<String>) listSchemaNames(session));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        CassandraTable table = cassandraSession.getTable(getTableName(tableHandle));
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (CassandraColumnHandle columnHandle : table.getColumns()) {
            columnHandles.put(cqlNameToSqlName(columnHandle.getName()).toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
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
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((CassandraColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        CassandraTableHandle handle = (CassandraTableHandle) tableHandle;

        CassandraPartitionResult partitionResult = partitionManager.getPartitions(handle, constraint.getSummary());

        String clusteringKeyPredicates = "";
        TupleDomain<ColumnHandle> unenforcedConstraint;
        if (partitionResult.isUnpartitioned()) {
            unenforcedConstraint = partitionResult.getUnenforcedConstraint();
        }
        else {
            CassandraClusteringPredicatesExtractor clusteringPredicatesExtractor = new CassandraClusteringPredicatesExtractor(
                    cassandraSession.getTable(handle.getSchemaTableName()).getClusteringKeyColumns(),
                    partitionResult.getUnenforcedConstraint(),
                    cassandraSession.getCassandraVersion());
            clusteringKeyPredicates = clusteringPredicatesExtractor.getClusteringKeyPredicates();
            unenforcedConstraint = clusteringPredicatesExtractor.getUnenforcedConstraints();
        }

        Optional<List<CassandraPartition>> currentPartitions = handle.getPartitions();
        if (currentPartitions.isPresent() &&
                // TODO: we should skip only when new table handle does not narrow down enforced predicate
                currentPartitions.get().containsAll(partitionResult.getPartitions()) &&
                handle.getClusteringKeyPredicates().equals(clusteringKeyPredicates)) {
            return Optional.empty();
        }

        return Optional.of(
                new ConstraintApplicationResult<>(new CassandraTableHandle(
                        handle.getSchemaName(),
                        handle.getTableName(),
                        Optional.of(partitionResult.getPartitions()),
                        clusteringKeyPredicates),
                        unenforcedConstraint));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of CassandraTableHandle");

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Cassandra catalog");
        }

        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;
        if (cassandraSession.isMaterializedView(cassandraTableHandle.getSchemaTableName())) {
            throw new PrestoException(NOT_SUPPORTED, "Dropping materialized views not yet supported");
        }

        cassandraSession.execute(format("DROP TABLE \"%s\".\"%s\"", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName()));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Renaming tables not yet supported for Cassandra");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return createTable(tableMetadata);
    }

    private CassandraOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<ExtraColumnMetadata> columnExtra = ImmutableList.builder();
        columnExtra.add(new ExtraColumnMetadata(ID_COLUMN_NAME, true));
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
            columnExtra.add(new ExtraColumnMetadata(column.getName(), column.isHidden()));
        }

        SchemaTableName table = tableMetadata.getTable();
        String schemaName = cassandraSession.getCaseSensitiveSchemaName(table.getSchemaName());
        String tableName = table.getTableName();
        List<String> columns = columnNames.build();
        List<Type> types = columnTypes.build();
        StringBuilder queryBuilder = new StringBuilder(format("CREATE TABLE \"%s\".\"%s\"(id uuid primary key", schemaName, tableName));
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            Type type = types.get(i);
            queryBuilder.append(", ")
                    .append(validColumnName(name))
                    .append(" ")
                    .append(toCassandraType(type, cassandraSession.getProtocolVersion()).name().toLowerCase(ENGLISH));
        }
        queryBuilder.append(") ");

        // encode column ordering in the cassandra table comment field since there is no better place to store this
        String columnMetadata = extraColumnMetadataCodec.toJson(columnExtra.build());
        queryBuilder.append("WITH comment='").append(PRESTO_COMMENT_METADATA).append(" ").append(columnMetadata).append("'");

        // We need to create the Cassandra table before commit because the record needs to be written to the table.
        cassandraSession.execute(queryBuilder.toString());
        return new CassandraOutputTableHandle(
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CassandraTableHandle table = (CassandraTableHandle) tableHandle;
        if (cassandraSession.isMaterializedView(table.getSchemaTableName())) {
            throw new PrestoException(NOT_SUPPORTED, "Inserting into materialized views not yet supported");
        }

        SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
        List<CassandraColumnHandle> columns = cassandraSession.getTable(schemaTableName).getColumns();
        List<String> columnNames = columns.stream()
                .filter(columnHandle -> !isHiddenIdColumn(columnHandle))
                .map(CassandraColumnHandle::getName)
                .collect(Collectors.toList());
        List<Type> columnTypes = columns.stream()
                .filter(columnHandle -> !isHiddenIdColumn(columnHandle))
                .map(CassandraColumnHandle::getType)
                .collect(Collectors.toList());
        boolean generateUuid = columns.stream()
                .filter(CassandraMetadata::isHiddenIdColumn)
                .collect(toOptional()) // must be at most one
                .isPresent();

        return new CassandraInsertTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                columnNames,
                columnTypes,
                generateUuid);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    private static boolean isHiddenIdColumn(CassandraColumnHandle columnHandle)
    {
        return columnHandle.isHidden() && ID_COLUMN_NAME.equals(columnHandle.getName());
    }
}
