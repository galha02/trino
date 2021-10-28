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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.exchange.ExchangeSink;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.execution.buffer.BufferState.FAILED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.OutputBuffers.BufferType.SPOOL;
import static io.trino.execution.buffer.PagesSerde.getSerializedPagePositionCount;
import static java.util.Objects.requireNonNull;

public class SpoolingExchangeOutputBuffer
        implements OutputBuffer
{
    private final StateMachine<BufferState> state;
    private final OutputBuffers outputBuffers;
    private final ExchangeSink exchangeSink;
    private final Supplier<LocalMemoryContext> memoryContextSupplier;

    private final AtomicLong peakMemoryUsage = new AtomicLong();
    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public SpoolingExchangeOutputBuffer(
            StateMachine<BufferState> state,
            OutputBuffers outputBuffers,
            ExchangeSink exchangeSink,
            Supplier<LocalMemoryContext> memoryContextSupplier)
    {
        this.state = requireNonNull(state, "state is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == SPOOL, "Expected a SPOOL output buffer");
        this.exchangeSink = requireNonNull(exchangeSink, "exchangeSink is null");
        this.memoryContextSupplier = requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        BufferState state = this.state.get();
        return new OutputBufferInfo(
                "EXTERNAL",
                state,
                false,
                state.canAddPages(),
                exchangeSink.getMemoryUsage(),
                totalPagesAdded.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                ImmutableList.of());
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return 0;
    }

    @Override
    public boolean isOverutilized()
    {
        return false;
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge(OutputBuffers.OutputBufferId bufferId, long token)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> isFull()
    {
        return asVoid(toListenableFuture(exchangeSink.isBlocked()));
    }

    @Override
    public void enqueue(List<Slice> pages)
    {
        enqueue(0, pages);
    }

    @Override
    public void enqueue(int partition, List<Slice> pages)
    {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return;
        }

        for (Slice page : pages) {
            exchangeSink.add(partition, page);
            totalRowsAdded.addAndGet(getSerializedPagePositionCount(page));
        }
        updateMemoryUsage(exchangeSink.getMemoryUsage());
        totalPagesAdded.addAndGet(pages.size());
    }

    @Override
    public void setNoMorePages()
    {
        if (state.compareAndSet(NO_MORE_BUFFERS, FLUSHING)) {
            destroy();
        }
    }

    @Override
    public void destroy()
    {
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            try {
                exchangeSink.finish();
            }
            finally {
                updateMemoryUsage(exchangeSink.getMemoryUsage());
            }
        }
    }

    @Override
    public void fail()
    {
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            try {
                exchangeSink.abort();
            }
            finally {
                updateMemoryUsage(0);
            }
        }
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }

    private void updateMemoryUsage(long bytes)
    {
        LocalMemoryContext context = getSystemMemoryContextOrNull();
        if (context != null) {
            context.setBytes(bytes);
        }
        updatePeakMemoryUsage(bytes);
    }

    private void updatePeakMemoryUsage(long bytes)
    {
        while (true) {
            long currentValue = peakMemoryUsage.get();
            if (currentValue >= bytes) {
                return;
            }
            if (peakMemoryUsage.compareAndSet(currentValue, bytes)) {
                return;
            }
        }
    }

    private LocalMemoryContext getSystemMemoryContextOrNull()
    {
        try {
            return memoryContextSupplier.get();
        }
        catch (RuntimeException ignored) {
            // This is possible with races, e.g., a task is created and then immediately aborted,
            // so that the task context hasn't been created yet (as a result there's no memory context available).
            return null;
        }
    }
}
