/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.BlockWriter;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.BlockWriterFactory;
import com.starburstdata.trino.plugins.snowflake.parallel.writer.ConverterFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.RootAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.FieldVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class SnowflakeArrowPageSource
        implements ConnectorPageSource
{
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator();
    private final BufferAllocator bufferAllocator;
    private final PageBuilder pageBuilder;
    private final List<JdbcColumnHandle> columns;
    private final StarburstDataConversionContext conversionContext;
    private final ChunkFileFetcher fetcher;
    private final long splitRetainedSize;
    private CompletableFuture<InputStream> downloadFuture;
    private long completedBytes;
    private boolean finished;

    public SnowflakeArrowPageSource(SnowflakeArrowSplit split, List<JdbcColumnHandle> columns, StarburstResultStreamProvider streamProvider)
    {
        this.splitRetainedSize = requireNonNull(split, "split is null").getRetainedSizeInBytes();
        this.columns = requireNonNull(columns, "columns is null");

        this.pageBuilder = new PageBuilder(columns.stream()
                .map(JdbcColumnHandle::getColumnType)
                .collect(toImmutableList()));

        this.bufferAllocator = ROOT_ALLOCATOR.newChildAllocator(
                "snowflakeArrowSplit" + split.hashCode(),
                split.getUncompressedByteSize() == 0 ? 1024 : split.getUncompressedByteSize(),
                Long.MAX_VALUE);

        int[] decimalColumnScales = columns.stream()
                .map(column -> column.getJdbcTypeHandle().getDecimalDigits()
                        .orElse(0))
                .mapToInt(Integer::intValue)
                .toArray();

        this.conversionContext = new StarburstDataConversionContext(
                split.getSnowflakeSessionParameters(),
                decimalColumnScales,
                split.getResultVersion());

        this.fetcher = new ChunkFileFetcher(requireNonNull(streamProvider, "streamProvider is null"), split);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return fetcher.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return downloadFuture == null ? NOT_BLOCKED : downloadFuture;
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");

        // getNextPage is not called concurrently hence there is no need for synchronization here
        if (!fetcher.startedFetching()) {
            this.downloadFuture = fetcher.startFetching();
            return null;
        }
        if (!downloadFuture.isDone()) {
            return null;
        }

        try (CloseableArrowBatch batch = decodeArrowInputStream(downloadFuture.get())) {
            for (List<ValueVector> vectors : batch.batch()) {
                int columnCount = columns.size();
                checkState(!vectors.isEmpty(), "There must be at least one vector in the batch of vectors");
                pageBuilder.declarePositions(vectors.get(0).getValueCount());
                Map<Integer, Integer> columnToVectorOrder = buildColumnOrder(vectors);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    BlockWriter writer = createWriter(vectors.get(columnToVectorOrder.get(columnIndex)), columnIndex);
                    writer.write(pageBuilder.getBlockBuilder(columnIndex));
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(JDBC_ERROR, "Failed reading Arrow stream", e);
        }
        catch (SFException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't write Snowflake blocks", e);
        }
        catch (ExecutionException | InterruptedException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't fetch chunk files", e);
        }
        finally {
            closeAllocator();
        }

        Page page = pageBuilder.build();
        // A single split maps to a chunk file, which holds up to a certain amount of records (from a few hundred up to a few million)
        pageBuilder.reset();
        completedBytes = page.getSizeInBytes();
        finished = true;

        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return bufferAllocator.getAllocatedMemory() + splitRetainedSize + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        if (downloadFuture != null) {
            downloadFuture.cancel(true);
            downloadFuture = null;
        }
        closeAllocator();
    }

    private BlockWriter createWriter(ValueVector vector, int columnIndex)
    {
        ArrowVectorConverter converter = ConverterFactory.createSnowflakeConverter(vector, columnIndex, conversionContext);
        JdbcColumnHandle columnHandle = columns.get(columnIndex);
        int rowCount = vector.getValueCount();
        return BlockWriterFactory.createWriter(columnHandle, converter, rowCount);
    }

    private Map<Integer, Integer> buildColumnOrder(List<ValueVector> vectors)
    {
        Map<Integer, Integer> columnToVectorOrder = new HashMap<>();
        for (JdbcColumnHandle column : columns) {
            ValueVector vector = vectors.stream().filter(valueVector -> column.getColumnName().equals(valueVector.getField().getName()))
                    .findAny()
                    .orElseThrow();
            // With this we write columns in the right order
            columnToVectorOrder.put(columns.indexOf(column), vectors.indexOf(vector));
        }
        return columnToVectorOrder;
    }

    private void closeAllocator()
    {
        bufferAllocator.close();
    }

    private CloseableArrowBatch decodeArrowInputStream(InputStream inputStream)
            throws IOException
    {
        try (ArrowStreamReader reader = new ArrowStreamReader(inputStream, bufferAllocator); VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
            ImmutableList.Builder<List<ValueVector>> batchBuilder = ImmutableList.builder();
            while (reader.loadNextBatch()) {
                ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builderWithExpectedSize(vectorSchemaRoot.getFieldVectors().size());
                for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
                    // transfer will not copy data but transfer ownership of memory, otherwise values will be gone
                    // once reader is gone
                    TransferPair transferPair = fieldVector.getTransferPair(bufferAllocator);
                    transferPair.transfer();
                    vectorBuilder.add(transferPair.getTo());
                }
                batchBuilder.add(vectorBuilder.build());
                vectorSchemaRoot.clear();
            }
            return new CloseableArrowBatch(batchBuilder.build());
        }
    }

    @SuppressWarnings("UnusedVariable") // error-prone false positive
    private record CloseableArrowBatch(List<List<ValueVector>> batch)
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            for (List<ValueVector> vectors : batch) {
                vectors.forEach(ValueVector::close);
            }
        }
    }
}
