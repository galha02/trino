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
package io.prestosql.orc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcWriteValidation.StatisticsValidation;
import io.prestosql.orc.OrcWriteValidation.WriteChecksum;
import io.prestosql.orc.OrcWriteValidation.WriteChecksumBuilder;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;
import io.prestosql.orc.metadata.PostScript.HiveWriterVersion;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;
import io.prestosql.orc.reader.StreamReader;
import io.prestosql.orc.reader.StreamReaders;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static io.prestosql.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static io.prestosql.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class OrcRecordReader
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcRecordReader.class).instanceSize();

    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private final long maxBlockBytes;
    private long currentPosition;
    private long currentStripePosition;
    private int currentBatchSize;
    private int nextBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;
    private AggregatedMemoryContext currentStripeSystemMemoryContext;

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private long filePosition;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private int currentRowGroup = -1;
    private long currentGroupRowCount;
    private long nextRowInGroup;

    private final Map<String, Slice> userMetadata;

    private final AggregatedMemoryContext systemMemoryUsage;

    private final Optional<OrcWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<StatisticsValidation> rowGroupStatisticsValidation;
    private final Optional<StatisticsValidation> stripeStatisticsValidation;
    private final Optional<StatisticsValidation> fileStatisticsValidation;

    public OrcRecordReader(
            Map<Integer, Type> includedColumns,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            OrcReaderOptions options,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize)
            throws OrcCorruptionException
    {
        requireNonNull(includedColumns, "includedColumns is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(types, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(includedColumns));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.systemMemoryUsage = systemMemoryUsage.newAggregatedMemoryContext();

        // reduce the included columns to the set that is also present
        ImmutableSet.Builder<Integer> presentColumns = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, Type> presentColumnsAndTypes = ImmutableMap.builder();
        OrcType root = types.get(0);
        for (Map.Entry<Integer, Type> entry : includedColumns.entrySet()) {
            // an old file can have less columns since columns can be added
            // after the file was written
            if (entry.getKey() < root.getFieldCount()) {
                presentColumns.add(entry.getKey());
                presentColumnsAndTypes.put(entry.getKey(), entry.getValue());
            }
        }
        this.presentColumns = presentColumns.build();

        requireNonNull(options, "options is null");
        this.maxBlockBytes = options.getMaxBlockSize().toBytes();

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = Optional.of(stripeStats.get(i));
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        stripeInfos.sort(comparingLong(info -> info.getStripe().getOffset()));

        long totalRowCount = 0;
        long fileRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        ImmutableList.Builder<Long> stripeFilePositions = ImmutableList.builder();
        if (predicate.matches(numberOfRows, getStatisticsByColumnOrdinal(root, fileStats))) {
            // select stripes that start within the specified split
            for (StripeInfo info : stripeInfos) {
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(root, stripe, info.getStats(), predicate)) {
                    stripes.add(stripe);
                    stripeFilePositions.add(fileRowCount);
                    totalRowCount += stripe.getNumberOfRows();
                }
                fileRowCount += stripe.getNumberOfRows();
            }
        }
        this.totalRowCount = totalRowCount;
        this.stripes = stripes.build();
        this.stripeFilePositions = stripeFilePositions.build();

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, options.getMaxMergeDistance(), options.getTinyStripeThreshold());
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.currentStripeSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();
        // The streamReadersSystemMemoryContext covers the StreamReader local buffer sizes, plus leaf node StreamReaders'
        // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
        // doesn't have a local buffer. All non-leaf level StreamReaders' (e.g. MapStreamReader, LongStreamReader,
        // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
        // their constructors is confusing.
        AggregatedMemoryContext streamReadersSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();

        stripeReader = new StripeReader(
                orcDataSource,
                hiveStorageTimeZone.toTimeZone().toZoneId(),
                decompressor,
                types,
                this.presentColumns,
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                writeValidation);

        streamReaders = createStreamReaders(orcDataSource, types, presentColumnsAndTypes.build(), streamReadersSystemMemoryContext);
        maxBytesPerCell = new long[streamReaders.length];
        nextBatchSize = initialBatchSize;
    }

    private static boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    private static boolean isStripeIncluded(
            OrcType rootStructType,
            StripeInformation stripe,
            Optional<StripeStatistics> stripeStats,
            OrcPredicate predicate)
    {
        // if there are no stats, include the column
        return stripeStats
                .map(StripeStatistics::getColumnStatistics)
                .map(columnStats -> getStatisticsByColumnOrdinal(rootStructType, columnStats))
                .map(statsByColumn -> predicate.matches(stripe.getNumberOfRows(), statsByColumn))
                .orElse(true);
    }

    @VisibleForTesting
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > tinyStripeThreshold.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, createTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold));
    }

    /**
     * Return the row position relative to the start of the file.
     */
    public long getFilePosition()
    {
        return filePosition;
    }

    /**
     * Returns the total number of rows in the file. This count includes rows
     * for stripes that were completely excluded due to stripe statistics.
     */
    public long getFileRowCount()
    {
        return fileRowCount;
    }

    /**
     * Return the row position within the stripes being read by this reader.
     * This position will include rows that were never read due to row groups
     * that are excluded due to row group statistics. Thus, it will advance
     * faster than the number of rows actually read.
     */
    public long getReaderPosition()
    {
        return currentPosition;
    }

    /**
     * Returns the total number of rows that can possibly be read by this reader.
     * This count may be fewer than the number of rows in the file if some
     * stripes were excluded due to stripe statistics, but may be more than
     * the number of rows read if some row groups are excluded due to statistics.
     */
    public long getReaderRowCount()
    {
        return totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(orcDataSource);
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    closer.register(column::close);
                }
            }
        }

        if (writeChecksumBuilder.isPresent()) {
            WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getStripeHash() == actualChecksum.getStripeHash(), "Invalid stripes checksum");
        }
        if (fileStatisticsValidation.isPresent()) {
            List<ColumnStatistics> columnStatistics = fileStatisticsValidation.get().build();
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), columnStatistics);
        }
    }

    public boolean isColumnPresent(int hiveColumnIndex)
    {
        return presentColumns.contains(hiveColumnIndex);
    }

    public int nextBatch()
            throws IOException
    {
        // update position for current row group (advancing resets them)
        filePosition += currentBatchSize;
        currentPosition += currentBatchSize;

        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                filePosition = fileRowCount;
                currentPosition = totalRowCount;
                return -1;
            }
        }

        // We will grow currentBatchSize by BATCH_SIZE_GROWTH_FACTOR starting from initialBatchSize to maxBatchSize or
        // the number of rows left in this rowgroup, whichever is smaller. maxBatchSize is adjusted according to the
        // block size for every batch and never exceed MAX_BATCH_SIZE. But when the number of rows in the last batch in
        // the current rowgroup is smaller than min(nextBatchSize, maxBatchSize), the nextBatchSize for next batch in
        // the new rowgroup should be grown based on min(nextBatchSize, maxBatchSize) but not by the number of rows in
        // the last batch, i.e. currentGroupRowCount - nextRowInGroup. For example, if the number of rows read for
        // single fixed width column are: 1, 16, 256, 1024, 1024,..., 1024, 256 and the 256 was because there is only
        // 256 rows left in this row group, then the nextBatchSize should be 1024 instead of 512. So we need to grow the
        // nextBatchSize before limiting the currentBatchSize by currentGroupRowCount - nextRowInGroup.
        currentBatchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(currentBatchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE);
        currentBatchSize = toIntExact(min(currentBatchSize, currentGroupRowCount - nextRowInGroup));

        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.prepareNextRead(currentBatchSize);
            }
        }
        nextRowInGroup += currentBatchSize;

        validateWritePageChecksum();
        return currentBatchSize;
    }

    public Block readBlock(int columnIndex)
            throws IOException
    {
        Block block = streamReaders[columnIndex].readBlock();
        if (block.getPositionCount() > 0) {
            long bytesPerCell = block.getSizeInBytes() / block.getPositionCount();
            if (maxBytesPerCell[columnIndex] < bytesPerCell) {
                maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[columnIndex] + bytesPerCell;
                maxBytesPerCell[columnIndex] = bytesPerCell;
                maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / maxCombinedBytesPerRow)));
            }
        }
        return block;
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        if (currentRowGroup >= 0) {
            if (rowGroupStatisticsValidation.isPresent()) {
                StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateRowGroupStatistics(orcDataSource.getId(), offset, currentRowGroup, statisticsValidation.build());
                statisticsValidation.reset();
            }
        }
        while (!rowGroups.hasNext() && currentStripe < stripes.size()) {
            advanceToNextStripe();
            currentRowGroup = -1;
        }

        if (!rowGroups.hasNext()) {
            currentGroupRowCount = 0;
            return false;
        }

        currentRowGroup++;
        RowGroup currentRowGroup = rowGroups.next();
        currentGroupRowCount = currentRowGroup.getRowCount();
        if (currentRowGroup.getMinAverageRowBytes() > 0) {
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / currentRowGroup.getMinAverageRowBytes())));
        }

        currentPosition = currentStripePosition + currentRowGroup.getRowOffset();
        filePosition = stripeFilePositions.get(currentStripe) + currentRowGroup.getRowOffset();

        // give reader data streams from row group
        InputStreamSources rowGroupStreamSources = currentRowGroup.getStreamSources();
        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeSystemMemoryContext.close();
        currentStripeSystemMemoryContext = systemMemoryUsage.newAggregatedMemoryContext();
        rowGroups = ImmutableList.<RowGroup>of().iterator();

        if (currentStripe >= 0) {
            if (stripeStatisticsValidation.isPresent()) {
                StatisticsValidation statisticsValidation = stripeStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateStripeStatistics(orcDataSource.getId(), offset, statisticsValidation.build());
                statisticsValidation.reset();
            }
        }

        currentStripe++;
        if (currentStripe >= stripes.size()) {
            return;
        }

        if (currentStripe > 0) {
            currentStripePosition += stripes.get(currentStripe - 1).getNumberOfRows();
        }

        StripeInformation stripeInformation = stripes.get(currentStripe);
        validateWriteStripe(stripeInformation.getNumberOfRows());

        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeSystemMemoryContext);
        if (stripe != null) {
            // Give readers access to dictionary streams
            InputStreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            List<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            ZoneId fileTimeZone = stripe.getFileTimeZone();
            ZoneId storageTimeZone = stripe.getStorageTimeZone();
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    column.startStripe(fileTimeZone, storageTimeZone, dictionaryStreamSources, columnEncodings);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(int rowCount)
    {
        writeChecksumBuilder.ifPresent(builder -> builder.addStripe(rowCount));
    }

    private void validateWritePageChecksum()
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            Block[] blocks = new Block[streamReaders.length];
            for (int columnIndex = 0; columnIndex < streamReaders.length; columnIndex++) {
                blocks[columnIndex] = readBlock(columnIndex);
            }
            Page page = new Page(currentBatchSize, blocks);
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.get().addPage(page);
            stripeStatisticsValidation.get().addPage(page);
            fileStatisticsValidation.get().addPage(page);
        }
    }

    private static StreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            Map<Integer, Type> includedColumns,
            AggregatedMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        StreamReader[] streamReaders = new StreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            Type type = includedColumns.get(columnId);
            if (type != null) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                streamReaders[columnId] = StreamReaders.createStreamReader(type, streamDescriptor, systemMemoryContext);
            }
        }
        return streamReaders;
    }

    private static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<OrcType> types, OrcDataSource dataSource)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type.getOrcTypeKind(), dataSource, nestedStreams.build());
    }

    private static Map<Integer, ColumnStatistics> getStatisticsByColumnOrdinal(OrcType rootStructType, List<ColumnStatistics> fileStats)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        requireNonNull(fileStats, "fileStats is null");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            if (fileStats.size() > ordinal) {
                ColumnStatistics element = fileStats.get(rootStructType.getFieldTypeIndex(ordinal));
                if (element != null) {
                    statistics.put(ordinal, element);
                }
            }
        }
        return statistics.build();
    }

    /**
     * @return The size of memory retained by all the stream readers (local buffers + object overhead)
     */
    @VisibleForTesting
    long getStreamReaderRetainedSizeInBytes()
    {
        long totalRetainedSizeInBytes = 0;
        for (StreamReader column : streamReaders) {
            if (column != null) {
                totalRetainedSizeInBytes += column.getRetainedSizeInBytes();
            }
        }
        return totalRetainedSizeInBytes;
    }

    /**
     * @return The size of memory retained by the current stripe (excludes object overheads)
     */
    @VisibleForTesting
    long getCurrentStripeRetainedSizeInBytes()
    {
        return currentStripeSystemMemoryContext.getBytes();
    }

    /**
     * @return The total size of memory retained by this OrcRecordReader
     */
    @VisibleForTesting
    long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getStreamReaderRetainedSizeInBytes() + getCurrentStripeRetainedSizeInBytes();
    }

    /**
     * @return The system memory reserved by this OrcRecordReader. It does not include non-leaf level StreamReaders'
     * instance sizes.
     */
    @VisibleForTesting
    long getSystemMemoryUsage()
    {
        return systemMemoryUsage.getBytes();
    }

    private static class StripeInfo
    {
        private final StripeInformation stripe;
        private final Optional<StripeStatistics> stats;

        public StripeInfo(StripeInformation stripe, Optional<StripeStatistics> stats)
        {
            this.stripe = requireNonNull(stripe, "stripe is null");
            this.stats = requireNonNull(stats, "metadata is null");
        }

        public StripeInformation getStripe()
        {
            return stripe;
        }

        public Optional<StripeStatistics> getStats()
        {
            return stats;
        }
    }

    static class LinearProbeRangeFinder
            implements CachingOrcDataSource.RegionFinder
    {
        private final List<DiskRange> diskRanges;
        private int index;

        private LinearProbeRangeFinder(List<DiskRange> diskRanges)
        {
            this.diskRanges = diskRanges;
        }

        @Override
        public DiskRange getRangeFor(long desiredOffset)
        {
            // Assumption: range are always read in order
            // Assumption: bytes that are not part of any range are never read
            while (index < diskRanges.size()) {
                DiskRange range = diskRanges.get(index);
                if (range.getEnd() > desiredOffset) {
                    checkArgument(range.getOffset() <= desiredOffset);
                    return range;
                }
                index++;
            }
            throw new IllegalArgumentException("Invalid desiredOffset " + desiredOffset);
        }

        public static LinearProbeRangeFinder createTinyStripesRangeFinder(List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
        {
            if (stripes.isEmpty()) {
                return new LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> diskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, tinyStripeThreshold);

            return new LinearProbeRangeFinder(diskRanges);
        }
    }
}
