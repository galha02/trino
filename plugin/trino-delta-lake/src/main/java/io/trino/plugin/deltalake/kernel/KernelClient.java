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
package io.trino.plugin.deltalake.kernel;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.types.StructType;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.kernel.clients.KernelTableClient;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.kernel.KernelSchemaUtils.toTrinoType;

public class KernelClient
{
    private KernelClient() {}

    private static DefaultTableClient tableClient = DefaultTableClient.create(new Configuration());
    private static final Map<SnapshotCacheKey, Snapshot> snapshotCache = new ConcurrentHashMap<>();

    public static Scan createScan(KernelDeltaLakeTableHandle tableHandle)
    {
        return tableHandle.getDeltaSnapshot().orElseThrow()
                .getScanBuilder(tableClient)
                // TODO: extract projects and filters from tableHandle
                .build();
    }

    public static Optional<Snapshot> getSnapshot(String tableLocation)
    {
        try {
            Table deltaTable = Table.forPath(tableClient, tableLocation);
            return Optional.of(deltaTable.getLatestSnapshot(tableClient));
        }
        catch (Exception e) {
            // TODO: Log
            return Optional.empty();
        }
    }

    public static List<ColumnMetadata> getTableColumnMetadata(
            SchemaTableName tableName,
            Snapshot snapshot,
            TypeManager typeManager)
    {
        StructType schema = snapshot.getSchema(tableClient);
        return schema.fields().stream()
                .map(field ->
                        new ColumnMetadata(
                                field.getName(),
                                toTrinoType(tableName, typeManager, field.getDataType())))
                .collect(toImmutableList());
    }

    public static TableClient getTableClient()
    {
        return tableClient;
    }

    public static TableClient getTableClient(
            Configuration configuration,
            TrinoFileSystem fileSystem,
            TypeManager typeManager)
    {
        return new KernelTableClient(configuration, fileSystem, typeManager);
    }

    public static Optional<Snapshot> getSnapshot(String tableLocation, long snapshotVersion)
    {
        SnapshotCacheKey snapshotCacheKey = new SnapshotCacheKey(tableLocation, snapshotVersion);
        return Optional.ofNullable(snapshotCache.computeIfAbsent(snapshotCacheKey, key -> {
            // TODO: should get the snapshot for the given version and not the latest snapshot
            Optional<Snapshot> snapshot = getSnapshot(tableLocation);
            if (snapshot.isEmpty()) {
                return null;
            }
            return snapshot.get();
        }));
    }

    public static List<String> getLowercasePartitionColumns(Snapshot snapshot)
    {
        Scan scan = snapshot.getScanBuilder(tableClient).build();
        scan.getScanState(tableClient);
        return null;
    }

    public static long getVersion(Snapshot snapshot)
    {
        return snapshot.getVersion(tableClient);
    }

    public static StructType getSchema(Snapshot snapshot)
    {
        return snapshot.getSchema(tableClient);
    }

    static class SnapshotCacheKey
    {
        private final String tableLocation;
        private final long snapshotVersion;

        public SnapshotCacheKey(String tableLocation, long snapshotVersion)
        {
            this.tableLocation = tableLocation;
            this.snapshotVersion = snapshotVersion;
        }

        public String getTableLocation()
        {
            return tableLocation;
        }

        public long getSnapshotVersion()
        {
            return snapshotVersion;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotCacheKey that = (SnapshotCacheKey) o;
            return snapshotVersion == that.snapshotVersion && Objects.equals(tableLocation, that.tableLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, snapshotVersion);
        }
    }
}
