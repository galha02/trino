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
import io.airlift.json.JsonCodec;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.tracer.HiveTracerFactory;
import io.prestosql.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.prestosql.spi.connector.ConnectorOperationContext;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.eventlistener.TracerEvent;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.tracer.ConnectorEventEmitter;
import io.prestosql.spi.tracer.ConnectorTracer;
import io.prestosql.spi.tracer.DefaultTracer;
import io.prestosql.spi.tracer.Tracer;
import io.prestosql.spi.tracer.TracerEventTypeSupplier;
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
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.prestosql.plugin.hive.BackgroundHiveSplitLoader.getBucketNumber;
import static io.prestosql.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.prestosql.plugin.hive.HiveStorageFormat.CSV;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSession;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.tracer.HiveTracerEventType.LIST_FILE_STATUS_END;
import static io.prestosql.plugin.hive.tracer.HiveTracerEventType.LIST_FILE_STATUS_START;
import static io.prestosql.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.prestosql.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.predicate.TupleDomain.withColumnDomains;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

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
            new HiveColumnHandle("col1", HIVE_INT, INTEGER, 0, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKETING_V1, BUCKET_COUNT, ImmutableList.of()));

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty(), ImmutableMap.of());
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY, ImmutableMap.of());

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testCsv()
            throws Exception
    {
        assertSplitCount(CSV, ImmutableMap.of(), 33);
        assertSplitCount(CSV, ImmutableMap.of("skip.header.line.count", "1"), 1);
        assertSplitCount(CSV, ImmutableMap.of("skip.footer.line.count", "1"), 1);
        assertSplitCount(CSV, ImmutableMap.of("skip.header.line.count", "1", "skip.footer.line.count", "1"), 1);
    }

    private void assertSplitCount(HiveStorageFormat storageFormat, Map<String, String> tableProperties, int expectedSplitCount)
            throws Exception
    {
        Table table = table(
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.copyOf(tableProperties),
                StorageFormat.fromHiveStorageFormat(storageFormat));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatus(new Path(SAMPLE_PATH), new DataSize(2.0, GIGABYTE).toBytes())),
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drainSplits(hiveSplitSource).size(), expectedSplitCount);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
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
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
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
                                getRegularColumnHandles(PARTITIONED_TABLE, TYPE_MANAGER),
                                BUCKETING_V1,
                                BUCKET_COUNT,
                                BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
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

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, () -> hiveSplitSource.isFinished());
    }

    @Test
    public void testCachedDirectoryLister()
            throws Exception
    {
        CachingDirectoryLister cachingDirectoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), 1000, ImmutableList.of("test_dbname.test_table"));
        assertEquals(cachingDirectoryLister.getRequestCount(), 0);

        int totalCount = 1000;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(EXECUTOR.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            try {
                return drainSplits(hiveSplitSource);
            }
            finally {
                firstVisit.countDown();
            }
        }));

        for (int i = 0; i < totalCount - 1; i++) {
            futures.add(EXECUTOR.submit(() -> {
                firstVisit.await();
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
                HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertEquals(future.get().size(), TEST_FILES.size());
        }
        assertEquals(cachingDirectoryLister.getRequestCount(), totalCount);
        assertEquals(cachingDirectoryLister.getHitCount(), totalCount - 1);
        assertEquals(cachingDirectoryLister.getMissCount(), 1);
    }

    @Test
    public void testGetBucketNumber()
    {
        assertEquals(getBucketNumber("0234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("000234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_99"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0_copy_1"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_235847_87654_fn7s5_bucket-56789"), OptionalInt.of(56789));

        assertEquals(getBucketNumber("234_99"), OptionalInt.empty());
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
    }

    @Test(dataProvider = "testPropagateExceptionDataProvider", timeOut = 60_000)
    public void testPropagateException(boolean error, int threads)
    {
        AtomicBoolean iteratorUsedAfterException = new AtomicBoolean();

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                () -> new Iterator<HivePartitionMetadata>()
                {
                    private boolean threw;

                    @Override
                    public boolean hasNext()
                    {
                        iteratorUsedAfterException.compareAndSet(false, threw);
                        return !threw;
                    }

                    @Override
                    public HivePartitionMetadata next()
                    {
                        iteratorUsedAfterException.compareAndSet(false, threw);
                        threw = true;
                        if (error) {
                            throw new Error("loading error occurred");
                        }
                        throw new RuntimeException("loading error occurred");
                    }
                },
                TupleDomain.all(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                SESSION,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                EXECUTOR,
                threads,
                false,
                false,
                Optional.empty(),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThatThrownBy(() -> drain(hiveSplitSource))
                .hasMessageEndingWith("loading error occurred");

        assertThatThrownBy(hiveSplitSource::isFinished)
                .hasMessageEndingWith("loading error occurred");

        if (threads == 1) {
            assertFalse(iteratorUsedAfterException.get());
        }
    }

    @DataProvider
    public Object[][] testPropagateExceptionDataProvider()
    {
        return new Object[][] {
                {false, 1},
                {true, 1},
                {false, 2},
                {true, 2},
                {false, 4},
                {true, 4},
        };
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactions()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of(
                        "transactional", "true",
                        "transactional_properties", "insert_only"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000001_0000001_0000/bucket_00000",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000",
                tablePath + "/delta_0000003_0000003_0000/bucket_00000");

        try {
            for (String path : filePaths) {
                File file = new File(path);
                assertTrue(file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
                file.createNewFile();
            }

            // ValidWriteIdList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
            // This writeId list has high watermark transaction=3 and aborted transaction=2
            String validWriteIdsList = format("4$%s.%s:3:9223372036854775807::2", table.getDatabaseName(), table.getTableName());

            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    HDFS_ENVIRONMENT,
                    TupleDomain.none(),
                    Optional.empty(),
                    table,
                    Optional.empty(),
                    Optional.of(new ValidReaderWriteIdList(validWriteIdsList)));

            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            List<String> splits = drain(hiveSplitSource);
            assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(0))), format("%s not found in splits %s", filePaths.get(0), splits));
            assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(2))), format("%s not found in splits %s", filePaths.get(2), splits));
        }
        finally {
            Files.walk(tablePath).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    @Test
    public void testTracerEvents()
            throws Exception
    {
        JsonCodec<Map<String, Object>> jsonCodec = JsonCodec.mapJsonCodec(String.class, Object.class);
        List<TracerEvent> tracerEvents = Collections.synchronizedList(new ArrayList<>());
        Tracer engineTracer = DefaultTracer.createBasicTracer(tracerEvents::add, jsonCodec::toJson, "node", URI.create("http://test.com"), "query");

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderWithTracer(
                TEST_FILES,
                TupleDomain.none(),
                (new HiveTracerFactory()).createConnectorTracer(new ConnectorEventEmitter(engineTracer)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);

        checkHiveEvents(tracerEvents, ImmutableList.of(LIST_FILE_STATUS_START, LIST_FILE_STATUS_END));
    }

    private void checkHiveEvents(List<TracerEvent> tracerEvents, List<TracerEventTypeSupplier> expectedEventTypes)
    {
        List<String> eventTypes = tracerEvents.stream().map(TracerEvent::getEventType).collect(Collectors.toList());
        assertTrue(tracerEvents.size() == expectedEventTypes.size(), format("received %d events not matching expected: %d", tracerEvents.size(), expectedEventTypes.size()));
        assertTrue(expectedEventTypes.stream().map(TracerEventTypeSupplier::toTracerEventType).allMatch(eventTypes::contains), "did not receive all expected events");
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
        return backgroundHiveSplitLoader(
                files,
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(
                new TestingHdfsEnvironment(files),
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            HdfsEnvironment hdfsEnvironment,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                compactEffectivePredicate,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                SESSION,
                hdfsEnvironment,
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                EXECUTOR,
                2,
                false,
                false,
                validWriteIds,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(List<LocatedFileStatus> files, DirectoryLister directoryLister)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas = ImmutableList.of(
                new HivePartitionMetadata(
                        new HivePartition(new SchemaTableName("testSchema", "table_name")),
                        Optional.empty(),
                        ImmutableMap.of()));

        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(new DataSize(1.0, GIGABYTE)));

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                hivePartitionMetadatas,
                TupleDomain.none(),
                Optional.empty(),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                EXECUTOR,
                2,
                false,
                false,
                Optional.empty(),
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderWithTracer(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> tupleDomain,
            ConnectorTracer tracer)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));
        ConnectorOperationContext connectorOperationContext = (new ConnectorOperationContext.Builder()).withConnectorTracer(Optional.of(tracer)).build();
        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                hivePartitionMetadatas,
                tupleDomain,
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                SESSION,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                EXECUTOR,
                2,
                false,
                false,
                Optional.empty(),
                connectorOperationContext.getConnectorTracer());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(new DataSize(1.0, GIGABYTE)));

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                directExecutor(),
                2,
                false,
                false,
                Optional.empty(),
                Optional.empty());
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

    private static HiveSplitSource hiveSplitSource(HiveSplitLoader hiveSplitLoader)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                1,
                1,
                new DataSize(32, MEGABYTE),
                Integer.MAX_VALUE,
                hiveSplitLoader,
                EXECUTOR,
                new CounterStat());
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(location,
                partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        return table("hdfs://VOL1:9000/db_name/table_name",
                partitionColumns,
                bucketProperty,
                tableParameters,
                storageFormat);
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(storageFormat)
                .setLocation(location)
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName("test_table")
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(tableParameters)
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return locatedFileStatus(path, 0);
    }

    private static LocatedFileStatus locatedFileStatus(Path path, long fileLength)
    {
        return new LocatedFileStatus(
                fileLength,
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
                new BlockLocation[] {new BlockLocation(new String[1], new String[] {"localhost"}, 0, fileLength)});
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

    public static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(
                            new HdfsConfigurationInitializer(new HdfsConfig()),
                            ImmutableSet.of()),
                    new HdfsConfig(),
                    new NoHdfsAuthentication());
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public FileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = ImmutableList.copyOf(files);
        }

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
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
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
