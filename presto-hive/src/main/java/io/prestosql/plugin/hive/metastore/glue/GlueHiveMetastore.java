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
package io.prestosql.plugin.hive.metastore.glue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionNotFoundException;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.SchemaAlreadyExistsException;
import io.prestosql.plugin.hive.TableAlreadyExistsException;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.glue.converter.GlueInputConverter;
import io.prestosql.plugin.hive.metastore.glue.converter.GlueToPrestoConverter;
import io.prestosql.plugin.hive.util.HiveUtil;
import io.prestosql.plugin.hive.util.HiveWriteUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Comparators.lexicographical;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static io.prestosql.plugin.hive.aws.AwsCurrentRegionHolder.getCurrentRegionFromEC2Metadata;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.prestosql.plugin.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;
import static io.prestosql.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public class GlueHiveMetastore
        implements HiveMetastore
{
    private static final Logger log = Logger.get(GlueHiveMetastore.class);

    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final String WILDCARD_EXPRESSION = "";
    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    private static final int BATCH_CREATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 128;
    private static final Comparator<Partition> PARTITION_COMPARATOR =
            comparing(Partition::getValues, lexicographical(String.CASE_INSENSITIVE_ORDER));

    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final AWSGlueAsync glueClient;
    private final Optional<String> defaultDir;
    private final String catalogId;
    private final int partitionSegments;
    private final Executor executor;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private final GlueColumnStatisticsProvider columnStatisticsProvider;

    @Inject
    public GlueHiveMetastore(
            HdfsEnvironment hdfsEnvironment,
            GlueHiveMetastoreConfig glueConfig,
            GlueColumnStatisticsProvider columnStatisticsProvider,
            @ForGlueHiveMetastore Executor executor,
            @ForGlueHiveMetastore Optional<RequestHandler2> requestHandler)
    {
        requireNonNull(glueConfig, "glueConfig is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));
        this.glueClient = createAsyncGlueClient(glueConfig, requestHandler);
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.catalogId = glueConfig.getCatalogId().orElse(null);
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.executor = requireNonNull(executor, "executor is null");
        this.columnStatisticsProvider = requireNonNull(columnStatisticsProvider, "columnStatisticsProvider is null");
    }

    private static AWSGlueAsync createAsyncGlueClient(GlueHiveMetastoreConfig config, Optional<RequestHandler2> requestHandler)
    {
        ClientConfiguration clientConfig = new ClientConfiguration().withMaxConnections(config.getMaxGlueConnections());
        AWSGlueAsyncClientBuilder asyncGlueClientBuilder = AWSGlueAsyncClientBuilder.standard()
                .withClientConfiguration(clientConfig);

        requestHandler.ifPresent(asyncGlueClientBuilder::setRequestHandlers);

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            asyncGlueClientBuilder.setEndpointConfiguration(new EndpointConfiguration(
                    config.getGlueEndpointUrl().get(),
                    config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            asyncGlueClientBuilder.setRegion(config.getGlueRegion().get());
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            asyncGlueClientBuilder.setRegion(getCurrentRegionFromEC2Metadata().getName());
        }

        asyncGlueClientBuilder.setCredentials(getAwsCredentialsProvider(config));

        return asyncGlueClientBuilder.build();
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(GlueHiveMetastoreConfig config)
    {
        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
        }
        if (config.getIamRole().isPresent()) {
            return new STSAssumeRoleSessionCredentialsProvider
                    .Builder(config.getIamRole().get(), "presto-session")
                    .withExternalId(config.getExternalId().orElse(null))
                    .build();
        }
        if (config.getAwsCredentialsProvider().isPresent()) {
            return getCustomAWSCredentialsProvider(config.getAwsCredentialsProvider().get());
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    private static AWSCredentialsProvider getCustomAWSCredentialsProvider(String providerClass)
    {
        try {
            Object instance = Class.forName(providerClass).getConstructor().newInstance();
            if (!(instance instanceof AWSCredentialsProvider)) {
                throw new RuntimeException("Invalid credentials provider class: " + instance.getClass().getName());
            }
            return (AWSCredentialsProvider) instance;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            GetDatabaseResult result = stats.getGetDatabase().call(() ->
                    glueClient.getDatabase(new GetDatabaseRequest().withCatalogId(catalogId).withName(databaseName)));
            return Optional.of(GlueToPrestoConverter.convertDatabase(result.getDatabase()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return stats.getGetAllDatabases().call(() -> {
                List<String> databaseNames = new ArrayList<>();
                String nextToken = null;

                do {
                    GetDatabasesResult result = glueClient.getDatabases(new GetDatabasesRequest().withCatalogId(catalogId).withNextToken(nextToken));
                    nextToken = result.getNextToken();
                    result.getDatabaseList().forEach(database -> databaseNames.add(database.getName()));
                }
                while (nextToken != null);

                return databaseNames;
            });
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        try {
            GetTableResult result = stats.getGetTable().call(() ->
                    glueClient.getTable(new GetTableRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withName(tableName)));
            return Optional.of(GlueToPrestoConverter.convertTable(result.getTable(), databaseName));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return columnStatisticsProvider.getSupportedColumnStatistics(type);
    }

    private Table getExistingTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), columnStatisticsProvider.getTableColumnStatistics(table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return partitions.stream().collect(toImmutableMap(partition -> makePartitionName(table, partition), this::getPartitionStatistics));
    }

    private PartitionStatistics getPartitionStatistics(Partition partition)
    {
        return new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), columnStatisticsProvider.getPartitionColumnStatistics(partition));
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table table = getExistingTable(identity, databaseName, tableName);
        PartitionStatistics currentStatistics = getTableStatistics(identity, table);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        try {
            TableInput tableInput = GlueInputConverter.convertTable(table);
            tableInput.setParameters(updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics()));
            columnStatisticsProvider.updateTableColumnStatistics(tableInput, updatedStatistics.getColumnStatistics());
            glueClient.updateTable(new UpdateTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableInput(tableInput));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        List<String> partitionValues = toPartitionValues(partitionName);
        Partition partition = getPartition(identity, table, partitionValues)
                .orElseThrow(() -> new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName));

        PartitionStatistics currentStatistics = getPartitionStatistics(partition);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        try {
            PartitionInput partitionInput = GlueInputConverter.convertPartition(partition);
            partitionInput.setParameters(updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics()));
            columnStatisticsProvider.updatePartitionStatistics(partitionInput, updatedStatistics.getColumnStatistics());
            glueClient.updatePartition(new UpdatePartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(table.getDatabaseName())
                    .withTableName(table.getTableName())
                    .withPartitionValueList(partition.getValues())
                    .withPartitionInput(partitionInput));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName()), partitionValues);
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return stats.getGetAllTables().call(() -> {
                List<String> tableNames = new ArrayList<>();
                String nextToken = null;

                do {
                    GetTablesResult result = glueClient.getTables(new GetTablesRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withNextToken(nextToken));
                    result.getTableList().forEach(table -> tableNames.add(table.getName()));
                    nextToken = result.getNextToken();
                }
                while (nextToken != null);

                return tableNames;
            });
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return ImmutableList.of();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        // TODO
        throw new UnsupportedOperationException("getTablesWithParameter for GlueHiveMetastore is not implemented");
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        try {
            return stats.getGetAllViews().call(() -> {
                List<String> views = new ArrayList<>();
                String nextToken = null;

                do {
                    GetTablesResult result = glueClient.getTables(new GetTablesRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withNextToken(nextToken));
                    result.getTableList().stream()
                            .filter(table -> VIRTUAL_VIEW.name().equals(table.getTableType()))
                            .forEach(table -> views.add(table.getName()));
                    nextToken = result.getNextToken();
                }
                while (nextToken != null);

                return views;
            });
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return ImmutableList.of();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        if (!database.getLocation().isPresent() && defaultDir.isPresent()) {
            String databaseLocation = new Path(defaultDir.get(), database.getDatabaseName()).toString();
            database = Database.builder(database)
                    .setLocation(Optional.of(databaseLocation))
                    .build();
        }

        try {
            DatabaseInput databaseInput = GlueInputConverter.convertDatabase(database);
            stats.getCreateDatabase().call(() ->
                    glueClient.createDatabase(new CreateDatabaseRequest().withCatalogId(catalogId).withDatabaseInput(databaseInput)));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        if (database.getLocation().isPresent()) {
            HiveWriteUtils.createDirectory(hdfsContext, hdfsEnvironment, new Path(database.getLocation().get()));
        }
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        try {
            stats.getDropDatabase().call(() ->
                    glueClient.deleteDatabase(new DeleteDatabaseRequest().withCatalogId(catalogId).withName(databaseName)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database).withName(newDatabaseName);
            stats.getRenameDatabase().call(() ->
                    glueClient.updateDatabase(new UpdateDatabaseRequest()
                            .withCatalogId(catalogId)
                            .withName(databaseName)
                            .withDatabaseInput(renamedDatabase)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "setting the database owner is not supported by Glue");
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = GlueInputConverter.convertTable(table);
            stats.getCreateTable().call(() ->
                    glueClient.createTable(new CreateTableRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(table.getDatabaseName())
                            .withTableInput(input)));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        Table table = getExistingTable(identity, databaseName, tableName);

        try {
            stats.getDropTable().call(() ->
                    glueClient.deleteTable(new DeleteTableRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withName(tableName)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String tableLocation = table.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(tableLocation)) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(tableLocation), true);
        }
    }

    private static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals(MANAGED_TABLE.name());
    }

    private static void deleteDir(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, boolean recursive)
    {
        try {
            hdfsEnvironment.getFileSystem(context, path).delete(path, recursive);
        }
        catch (Exception e) {
            // don't fail if unable to delete path
            log.warn(e, "Failed to delete path: " + path.toString());
        }
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            stats.getReplaceTable().call(() ->
                    glueClient.updateTable(new UpdateTableRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withTableInput(newTableInput)));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Table rename is not yet supported by Glue service");
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        throw new PrestoException(NOT_SUPPORTED, "Table comment is not yet supported by Glue service");
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Table oldTable = getExistingTable(identity, databaseName, tableName);
        Table newTable = Table.builder(oldTable)
                .addDataColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                .build();
        replaceTable(identity, databaseName, tableName, newTable, null);
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Table oldTable = getExistingTable(identity, databaseName, tableName);
        if (oldTable.getPartitionColumns().stream().anyMatch(c -> c.getName().equals(oldColumnName))) {
            throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column column : oldTable.getDataColumns()) {
            if (column.getName().equals(oldColumnName)) {
                newDataColumns.add(new Column(newColumnName, column.getType(), column.getComment()));
            }
            else {
                newDataColumns.add(column);
            }
        }

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(identity, databaseName, tableName, newTable, null);
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, identity, databaseName, tableName, columnName);
        Table oldTable = getExistingTable(identity, databaseName, tableName);

        if (!oldTable.getColumn(columnName).isPresent()) {
            SchemaTableName name = new SchemaTableName(databaseName, tableName);
            throw new ColumnNotFoundException(name, columnName);
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        oldTable.getDataColumns().stream()
                .filter(fieldSchema -> !fieldSchema.getName().equals(columnName))
                .forEach(newDataColumns::add);

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(identity, databaseName, tableName, newTable, null);
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        try {
            GetPartitionResult result = stats.getGetPartition().call(() ->
                    glueClient.getPartition(new GetPartitionRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(table.getDatabaseName())
                            .withTableName(table.getTableName())
                            .withPartitionValues(partitionValues)));
            return Optional.of(GlueToPrestoConverter.convertPartition(result.getPartition(), table.getParameters()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        Table table = getExistingTable(identity, databaseName, tableName);
        List<Partition> partitions = getPartitions(table, WILDCARD_EXPRESSION);
        return Optional.of(buildPartitionNames(table.getPartitionColumns(), partitions));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b', 'c']
     *     Valid partition values:
     *     ['1','2','3'] or
     *     ['', '2', '']
     * </pre>
     *
     * @param parts Full or partial list of partition values to filter on. Keys without filter will be empty strings.
     * @return a list of partition names.
     */
    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        Table table = getExistingTable(identity, databaseName, tableName);
        String expression = buildGlueExpression(table.getPartitionColumns(), parts);
        List<Partition> partitions = getPartitions(table, expression);
        return Optional.of(buildPartitionNames(table.getPartitionColumns(), partitions));
    }

    private List<Partition> getPartitions(Table table, String expression)
    {
        if (partitionSegments == 1) {
            return getPartitions(table, expression, null);
        }

        // Do parallel partition fetch.
        CompletionService<List<Partition>> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < partitionSegments; i++) {
            Segment segment = new Segment().withSegmentNumber(i).withTotalSegments(partitionSegments);
            completionService.submit(() -> getPartitions(table, expression, segment));
        }

        List<Partition> partitions = new ArrayList<>();
        try {
            for (int i = 0; i < partitionSegments; i++) {
                Future<List<Partition>> futurePartitions = completionService.take();
                partitions.addAll(futurePartitions.get());
            }
        }
        catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, "Failed to fetch partitions from Glue Data Catalog", e);
        }

        partitions.sort(PARTITION_COMPARATOR);
        return partitions;
    }

    private List<Partition> getPartitions(Table table, String expression, @Nullable Segment segment)
    {
        try {
            return stats.getGetPartitions().call(() -> {
                List<Partition> partitions = new ArrayList<>();
                String nextToken = null;

                do {
                    GetPartitionsResult result = glueClient.getPartitions(new GetPartitionsRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(table.getDatabaseName())
                            .withTableName(table.getTableName())
                            .withExpression(expression)
                            .withSegment(segment)
                            .withNextToken(nextToken)
                            .withMaxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS));
                    result.getPartitions()
                            .forEach(partition -> partitions.add(GlueToPrestoConverter.convertPartition(partition, table.getParameters())));
                    nextToken = result.getNextToken();
                }
                while (nextToken != null);

                return partitions;
            });
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static List<String> buildPartitionNames(List<Column> partitionColumns, List<Partition> partitions)
    {
        return partitions.stream()
                .map(partition -> makePartitionName(partitionColumns, partition.getValues()))
                .collect(toList());
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b']
     *     Partition names = ['a=1/b=2', 'a=2/b=2']
     * </pre>
     *
     * @param partitionNames List of full partition names
     * @return Mapping of partition name to partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        return stats.getGetPartitionByName().call(() -> getPartitionsByNames(table, partitionNames));
    }

    private Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        List<Partition> partitions = batchGetPartition(table, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(toMap(identity(), HiveUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(Partition::getValues, identity()));

        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    private List<Partition> batchGetPartition(Table table, List<String> partitionNames)
    {
        try {
            List<PartitionValueList> partitionValueLists = partitionNames.stream()
                    .map(partitionName -> new PartitionValueList().withValues(toPartitionValues(partitionName))).collect(toList());

            List<List<PartitionValueList>> batchedPartitionValueLists = Lists.partition(partitionValueLists, BATCH_GET_PARTITION_MAX_PAGE_SIZE);
            List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = new ArrayList<>();
            List<Partition> result = new ArrayList<>();

            for (List<PartitionValueList> partitions : batchedPartitionValueLists) {
                batchGetPartitionFutures.add(glueClient.batchGetPartitionAsync(new BatchGetPartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(table.getDatabaseName())
                        .withTableName(table.getTableName())
                        .withPartitionsToGet(partitions)));
            }

            for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
                future.get().getPartitions()
                        .forEach(partition -> result.add(GlueToPrestoConverter.convertPartition(partition, table.getParameters())));
            }

            return result;
        }
        catch (AmazonServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            stats.getAddPartitions().call(() -> {
                List<List<PartitionWithStatistics>> batchedPartitions = Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE);
                List<Future<BatchCreatePartitionResult>> futures = new ArrayList<>();

                for (List<PartitionWithStatistics> partitionBatch : batchedPartitions) {
                    List<PartitionInput> partitionInputs = partitionBatch.stream()
                            .map(partition -> GlueInputConverter.convertPartition(partition, columnStatisticsProvider))
                            .collect(toList());
                    futures.add(glueClient.batchCreatePartitionAsync(new BatchCreatePartitionRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withTableName(tableName)
                            .withPartitionInputList(partitionInputs)));
                }

                for (Future<BatchCreatePartitionResult> future : futures) {
                    try {
                        BatchCreatePartitionResult result = future.get();
                        propagatePartitionErrorToPrestoException(databaseName, tableName, result.getErrors());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new PrestoException(HIVE_METASTORE_ERROR, e);
                    }
                }

                return null;
            });
        }
        catch (AmazonServiceException | ExecutionException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToPrestoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.get(0).getErrorDetail();
            String glueExceptionCode = errorDetail.getErrorCode();

            switch (glueExceptionCode) {
                case "AlreadyExistsException":
                    throw new PrestoException(ALREADY_EXISTS, errorDetail.getErrorMessage());
                case "EntityNotFoundException":
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), errorDetail.getErrorMessage());
                default:
                    throw new PrestoException(HIVE_METASTORE_ERROR, errorDetail.getErrorCode() + ": " + errorDetail.getErrorMessage());
            }
        }
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Table table = getExistingTable(identity, databaseName, tableName);
        Partition partition = getPartition(identity, table, parts)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts));

        try {
            stats.getDropPartition().call(() ->
                    glueClient.deletePartition(new DeletePartitionRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withTableName(tableName)
                            .withPartitionValues(parts)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(partLocation)) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(partLocation), true);
        }
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = GlueInputConverter.convertPartition(partition, columnStatisticsProvider);
            stats.getAlterPartition().call(() ->
                    glueClient.updatePartition(new UpdatePartitionRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(databaseName)
                            .withTableName(tableName)
                            .withPartitionInput(newPartition)
                            .withPartitionValueList(partition.getPartition().getValues())));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "createRole is not supported by Glue");
    }

    @Override
    public void dropRole(String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropRole is not supported by Glue");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of(PUBLIC_ROLE_NAME);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantRoles is not supported by Glue");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeRoles is not supported by Glue");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal.toPrestoPrincipal(), PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by Glue");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by Glue");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
