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
package io.trino.plugin.hive.metastore.recording;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.SortingColumn.Order;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRecordingHiveMetastore
{
    private static final Database DATABASE = new Database(
            "database",
            Optional.of("location"),
            Optional.of("owner"),
            Optional.of(USER),
            Optional.of("comment"),
            ImmutableMap.of("param", "value"));
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"),
            ImmutableMap.of());
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            Optional.of("location"),
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), BUCKETING_V1, 10, ImmutableList.of(new SortingColumn("column", Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));
    private static final Table TABLE = new Table(
            "database",
            "table",
            Optional.of("owner"),
            "table_type",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value3"),
            Optional.of("original_text"),
            Optional.of("expanded_text"),
            OptionalLong.empty());
    private static final Partition PARTITION = new Partition(
            "database",
            "table",
            ImmutableList.of("value"),
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value4"));
    private static final Partition OTHER_PARTITION = new Partition(
            "database",
            "table",
            ImmutableList.of("other_value"),
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value4"));
    public static final ImmutableMap<String, HiveColumnStatistics> COLUMN_STATISTICS = ImmutableMap.of("column", new HiveColumnStatistics(
            Optional.of(new IntegerStatistics(
                    OptionalLong.of(-100),
                    OptionalLong.of(102))),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            OptionalLong.of(1234),
            OptionalDouble.of(123.0),
            OptionalLong.of(1),
            OptionalLong.of(8)));
    private static final PartitionStatistics PARTITION_STATISTICS = new PartitionStatistics(
            new HiveBasicStatistics(10, 11, 10000, 10001),
            COLUMN_STATISTICS);
    private static final HivePrivilegeInfo PRIVILEGE_INFO = new HivePrivilegeInfo(HivePrivilege.SELECT, true, new HivePrincipal(USER, "grantor"), new HivePrincipal(USER, "grantee"));
    private static final RoleGrant ROLE_GRANT = new RoleGrant(new TrinoPrincipal(USER, "grantee"), "role", true);
    private static final List<String> PARTITION_COLUMN_NAMES = ImmutableList.of(TABLE_COLUMN.getName());
    private static final Domain PARTITION_COLUMN_EQUAL_DOMAIN = Domain.singleValue(createUnboundedVarcharType(), Slices.utf8Slice("value1"));
    private static final TupleDomain<String> TUPLE_DOMAIN = TupleDomain.withColumnDomains(ImmutableMap.of(TABLE_COLUMN.getName(), PARTITION_COLUMN_EQUAL_DOMAIN));

    @Test
    @Disabled // disabling as recording mechanism does not work correctly after statistics related changes to HiveMetastore interface
    // TODO: remove recording code altogether
    public void testRecordingHiveMetastore()
            throws IOException
    {
        RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                .setRecordingPath(File.createTempFile("recording_test", "json").getAbsolutePath())
                .setRecordingDuration(new Duration(10, TimeUnit.MINUTES));
        JsonCodec<HiveMetastoreRecording.Recording> jsonCodec = createJsonCodec();
        HiveMetastoreRecording recording = new HiveMetastoreRecording(recordingConfig, jsonCodec);
        RecordingHiveMetastore recordingHiveMetastore = new RecordingHiveMetastore(new TestingHiveMetastore(), recording);
        validateMetadata(recordingHiveMetastore);
        recordingHiveMetastore.dropDatabase("other_database", true);
        recording.writeRecording();

        RecordingMetastoreConfig replayingConfig = recordingConfig
                .setReplay(true);

        recording = new HiveMetastoreRecording(replayingConfig, jsonCodec);
        recordingHiveMetastore = new RecordingHiveMetastore(new UnimplementedHiveMetastore(), recording);
        recording.loadRecording();
        validateMetadata(recordingHiveMetastore);
        validatePartitionSubset(recordingHiveMetastore);
    }

    public static JsonCodec<HiveMetastoreRecording.Recording> createJsonCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        TypeDeserializer typeDeserializer = new TypeDeserializer(new TestingTypeManager());
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Block.class, new TestingBlockJsonSerde.Deserializer(new HiveBlockEncodingSerde()),
                        Type.class, typeDeserializer));
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class, new TestingBlockJsonSerde.Serializer(new HiveBlockEncodingSerde())));
        JsonCodec<HiveMetastoreRecording.Recording> jsonCodec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveMetastoreRecording.Recording.class);
        return jsonCodec;
    }

    private void validateMetadata(HiveMetastore hiveMetastore)
    {
        assertThat(hiveMetastore.getDatabase("database")).isEqualTo(Optional.of(DATABASE));
        assertThat(hiveMetastore.getAllDatabases()).isEqualTo(ImmutableList.of("database"));
        assertThat(hiveMetastore.getTable("database", "table")).isEqualTo(Optional.of(TABLE));
        assertThat(hiveMetastore.getTableColumnStatistics("database", "table", ImmutableSet.of("column"))).isEqualTo(PARTITION_STATISTICS);
        assertThat(hiveMetastore.getPartitionColumnStatistics(
                "database",
                "table",
                ImmutableSet.of("column=value", "column=other_value"),
                ImmutableSet.of("column")))
                .isEqualTo(ImmutableMap.of(
                        "column=value", PARTITION_STATISTICS,
                        "column=other_value", PARTITION_STATISTICS));
        assertThat(hiveMetastore.getTables("database")).isEqualTo(ImmutableList.of("table"));
        assertThat(hiveMetastore.getTablesWithParameter("database", "param", "value3")).isEqualTo(ImmutableList.of("table"));
        assertThat(hiveMetastore.getViews("database")).isEqualTo(ImmutableList.of());
        assertThat(hiveMetastore.getPartition(TABLE, ImmutableList.of("value"))).isEqualTo(Optional.of(PARTITION));
        assertThat(hiveMetastore.getPartitionNamesByFilter("database", "table", PARTITION_COLUMN_NAMES, TupleDomain.all())).isEqualTo(Optional.of(ImmutableList.of("value")));
        assertThat(hiveMetastore.getPartitionNamesByFilter("database", "table", PARTITION_COLUMN_NAMES, TUPLE_DOMAIN)).isEqualTo(Optional.of(ImmutableList.of("value")));
        assertThat(hiveMetastore.getPartitionsByNames(TABLE, ImmutableList.of("column=value", "column=other_value"))).isEqualTo(ImmutableMap.of(
                "column=value", Optional.of(PARTITION),
                "column=other_value", Optional.of(OTHER_PARTITION)));
        assertThat(hiveMetastore.listTablePrivileges("database", "table", Optional.of("owner"), Optional.of(new HivePrincipal(USER, "user")))).isEqualTo(ImmutableSet.of(PRIVILEGE_INFO));
        assertThat(hiveMetastore.listRoles()).isEqualTo(ImmutableSet.of("role"));
        assertThat(hiveMetastore.listRoleGrants(new HivePrincipal(USER, "user"))).isEqualTo(ImmutableSet.of(ROLE_GRANT));
    }

    private void validatePartitionSubset(HiveMetastore hiveMetastore)
    {
        assertThat(hiveMetastore.getPartitionColumnStatistics("database", "table",
                ImmutableSet.of("column=value"),
                ImmutableSet.of("column")))
                .isEqualTo(ImmutableMap.of("column=value", PARTITION_STATISTICS));
        assertThat(hiveMetastore.getPartitionColumnStatistics("database", "table",
                ImmutableSet.of("column=other_value"),
                ImmutableSet.of("column")))
                .isEqualTo(ImmutableMap.of("column=value", PARTITION_STATISTICS));
        assertThat(hiveMetastore.getPartitionsByNames(TABLE, ImmutableList.of("column=value"))).isEqualTo(ImmutableMap.of("column=value", Optional.of(PARTITION)));
        assertThat(hiveMetastore.getPartitionsByNames(TABLE, ImmutableList.of("column=other_value"))).isEqualTo(ImmutableMap.of("column=other_value", Optional.of(OTHER_PARTITION)));
    }

    private static class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(DATABASE);
            }

            return Optional.empty();
        }

        @Override
        public List<String> getAllDatabases()
        {
            return ImmutableList.of("database");
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(TABLE);
            }

            return Optional.empty();
        }

        @Override
        public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return COLUMN_STATISTICS;
            }

            return ImmutableMap.of();
        }

        @Override
        public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
                String databaseName,
                String tableName,
                Set<String> partitionNames,
                Set<String> columnNames)
        {
            ImmutableMap.Builder<String, Map<String, HiveColumnStatistics>> result = ImmutableMap.builder();
            if (databaseName.equals("database") && tableName.equals("table")) {
                if (partitionNames.stream().anyMatch(partition -> partition.equals("column=value"))) {
                    result.put("column=value", COLUMN_STATISTICS);
                }
                if (partitionNames.stream().anyMatch(partition -> partition.equals("column=other_value"))) {
                    result.put("column=other_value", COLUMN_STATISTICS);
                }
            }
            return result.buildOrThrow();
        }

        @Override
        public List<String> getTables(String databaseName)
        {
            if (databaseName.equals("database")) {
                return ImmutableList.of("table");
            }

            return ImmutableList.of();
        }

        @Override
        public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
        {
            if (databaseName.equals("database") && parameterKey.equals("param") && parameterValue.equals("value3")) {
                return ImmutableList.of("table");
            }
            return ImmutableList.of();
        }

        @Override
        public List<String> getViews(String databaseName)
        {
            return ImmutableList.of();
        }

        @Override
        public void dropDatabase(String databaseName, boolean deleteData)
        {
            // noop for test purpose
        }

        @Override
        public Optional<Partition> getPartition(Table table, List<String> partitionValues)
        {
            if (table.getDatabaseName().equals("database") && table.getTableName().equals("table")) {
                if (partitionValues.equals(ImmutableList.of("value"))) {
                    return Optional.of(PARTITION);
                }
                if (partitionValues.equals(ImmutableList.of("other_value"))) {
                    return Optional.of(OTHER_PARTITION);
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNamesByFilter(String databaseName,
                String tableName,
                List<String> columnNames,
                TupleDomain<String> partitionKeysFilter)
        {
            Domain filterDomain = partitionKeysFilter.getDomains().get().get(TABLE_COLUMN.getName());
            if (databaseName.equals("database") && tableName.equals("table") && (filterDomain == null || filterDomain.equals(PARTITION_COLUMN_EQUAL_DOMAIN))) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
        {
            ImmutableMap.Builder<String, Optional<Partition>> result = ImmutableMap.builder();
            if (table.getDatabaseName().equals("database") && table.getTableName().equals("table")) {
                if (partitionNames.contains("column=value")) {
                    result.put("column=value", Optional.of(PARTITION));
                }
                if (partitionNames.contains("column=other_value")) {
                    result.put("column=other_value", Optional.of(OTHER_PARTITION));
                }
            }
            return result.buildOrThrow();
        }

        @Override
        public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
        {
            if (databaseName.equals("database") && tableName.equals("table") && principal.get().getType() == USER && principal.get().getName().equals("user")) {
                return ImmutableSet.of(PRIVILEGE_INFO);
            }

            return ImmutableSet.of();
        }

        @Override
        public Set<String> listRoles()
        {
            return ImmutableSet.of("role");
        }

        @Override
        public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
        {
            return ImmutableSet.of(ROLE_GRANT);
        }
    }
}
