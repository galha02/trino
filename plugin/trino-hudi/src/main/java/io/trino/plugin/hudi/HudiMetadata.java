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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.util.HiveUtil.columnMetadataGetter;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hudi.HudiSessionProperties.getColumnsToHide;
import static io.trino.plugin.hudi.HudiTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.model.HudiTableType.COPY_ON_WRITE;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;

    public HudiMetadata(HiveMetastore metastore, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !isHiveSystemSchema(schemaName))
                .collect(toImmutableList());
    }

    @Override
    public HudiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }
        if (!isHudiTable(session, table.get())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Not a Hudi table: %s", tableName));
        }
        return new HudiTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().getStorage().getLocation(),
                COPY_ON_WRITE,
                TupleDomain.all(),
                TupleDomain.all());
    }

    private boolean isHudiTable(ConnectorSession session, Table table)
    {
        if (!MANAGED_TABLE.name().equals(table.getTableType()) && !EXTERNAL_TABLE.name().equals(table.getTableType())) {
            // Views are not Hudi tables
            return false;
        }
        if (table.getStorage().getOptionalLocation().isEmpty() || table.getStorage().getOptionalLocation().get().isEmpty()) {
            // No location or empty location cannot be a valid Hudi table
            return false;
        }
        return HudiUtil.isHudiTable(fileSystemFactory.create(session), Location.of(table.getStorage().getLocation()));
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(tableName, session)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(SchemaTableName tableName, ConnectorSession session)
    {
        HudiTableName name = HudiTableName.from(tableName.getTableName());
        if (name.getTableType() == TableType.DATA) {
            return Optional.empty();
        }

        Optional<Table> tableOptional = metastore.getTable(tableName.getSchemaName(), name.getTableName());
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }
        if (!isHudiTable(session, tableOptional.get())) {
            return Optional.empty();
        }
        switch (name.getTableType()) {
            case DATA:
                break;
            case TIMELINE:
                SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
                return Optional.of(new TimelineTable(fileSystemFactory.create(session), systemTableName, tableOptional.get()));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) table;
        return getTableMetadata(hudiTableHandle.getSchemaTableName(), getColumnsToHide(session));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        HudiPredicates predicates = HudiPredicates.from(constraint.getSummary());
        HudiTableHandle newHudiTableHandle = handle.applyPredicates(
                predicates.getPartitionColumnPredicates(),
                predicates.getRegularColumnPredicates());

        if (handle.getPartitionPredicates().equals(newHudiTableHandle.getPartitionPredicates())
                && handle.getRegularPredicates().equals(newHudiTableHandle.getRegularPredicates())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHudiTableHandle,
                newHudiTableHandle.getRegularPredicates().transformKeys(ColumnHandle.class::cast),
                false));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));
        return hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.of(HudiTableInfo.from((HudiTableHandle) table));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllTables(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        return tables.stream()
                .map(table -> getTableColumnMetadata(session, table))
                .flatMap(Optional::stream)
                .iterator();
    }

    HiveMetastore getMetastore()
    {
        return metastore;
    }

    private Optional<TableColumnsMetadata> getTableColumnMetadata(ConnectorSession session, SchemaTableName table)
    {
        try {
            List<ColumnMetadata> columns = getTableMetadata(table, getColumnsToHide(session)).getColumns();
            return Optional.of(TableColumnsMetadata.forTable(table, columns));
        }
        catch (TableNotFoundException ignored) {
            return Optional.empty();
        }
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName, Collection<String> columnsToHide)
    {
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table);
        List<ColumnMetadata> columns = hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .filter(column -> !columnsToHide.contains(column.getName()))
                .map(metadataGetter)
                .collect(toImmutableList());

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        // Location property
        String location = table.getStorage().getLocation();
        if (!isNullOrEmpty(location)) {
            properties.put(LOCATION_PROPERTY, location);
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(TABLE_COMMENT));
        return new ConnectorTableMetadata(tableName, columns, properties.buildOrThrow(), comment);
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName
                .filter(name -> !isHiveSystemSchema(name))
                .map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session));
    }
}
