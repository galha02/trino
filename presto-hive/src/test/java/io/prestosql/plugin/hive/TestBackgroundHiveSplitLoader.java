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
package io.prestosql.plugin.hive;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.HiveBucketing.HiveBucketFilter;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.prestosql.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getRegularColumnHandles;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.predicate.TupleDomain.withColumnDomains;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final Executor EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final TupleDomain<HiveColumnHandle> RETURNED_PATH_DOMAIN = withColumnDomains(
            ImmutableMap.of(
                    pathColumnHandle(),
                    Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()))));

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH),
            locatedFileStatus(FILTERED_PATH));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            new HiveColumnHandle("col1", HIVE_INT, INTEGER.getTypeSignature(), 0, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKET_COUNT, ImmutableList.of()));

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty());
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY);

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, TupleDomain.none());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.of(new HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKET_COUNT, BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE),
                                BUCKET_COUNT,
                                BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, RETURNED_PATH_DOMAIN);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatusWithNoBlocks(RETURNED_PATH)),
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, TupleDomain.none());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader, TupleDomain.all());
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, () -> hiveSplitSource.isFinished());
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            source.getNextBatch(NOT_PARTITIONED, 100).get()
                    .getSplits().stream()
                    .map(HiveSplit.class::cast)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> tupleDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                tupleDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                compactEffectivePredicate,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                connectorSession,
                new TestingHdfsEnvironment(),
                new NamenodeStats(),
                new TestingDirectoryLister(files),
                EXECUTOR,
                2,
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(),
                new NamenodeStats(),
                new TestingDirectoryLister(TEST_FILES),
                directExecutor(),
                2,
                false);
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<HivePartitionMetadata>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
            }
        };
    }

    private static HiveSplitSource hiveSplitSource(
            BackgroundHiveSplitLoader backgroundHiveSplitLoader,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                compactEffectivePredicate,
                1,
                1,
                new DataSize(32, MEGABYTE),
                backgroundHiveSplitLoader,
                EXECUTOR,
                new CounterStat());
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(
                        StorageFormat.create(
                                "com.facebook.hive.orc.OrcSerde",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                .setLocation("hdfs://VOL1:9000/db_name/table_name")
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName("test_table")
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of())
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation()});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    private static class TestingDirectoryLister
            implements DirectoryLister
    {
        private final List<LocatedFileStatus> files;

        public TestingDirectoryLister(List<LocatedFileStatus> files)
        {
            this.files = files;
        }

        @Override
        public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Path path)
        {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                {
                    return iterator.next();
                }
            };
        }
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        public TestingHdfsEnvironment()
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationUpdater(new HiveClientConfig()), ImmutableSet.of()),
                    new HiveClientConfig(),
                    new NoHdfsAuthentication());
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem();
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}
