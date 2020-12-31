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
package io.trino.split;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.split.MockSplitSource.Action.DO_NOTHING;
import static io.trino.split.MockSplitSource.Action.FINISH;

@NotThreadSafe
public class MockSplitSource
        implements SplitSource
{
    private static final Split SPLIT = new Split(new CatalogName("test"), new MockConnectorSplit(), Lifespan.taskWide());
    private static final SettableFuture<List<Split>> COMPLETED_FUTURE = SettableFuture.create();

    static {
        COMPLETED_FUTURE.set(null);
    }

    private int batchSize;
    private int totalSplits;
    private Action atSplitDepletion = DO_NOTHING;

    private int nextBatchInvocationCount;
    private int splitsProduced;

    private SettableFuture<List<Split>> nextBatchFuture = COMPLETED_FUTURE;
    private int nextBatchMaxSize;

    public MockSplitSource()
    {
    }

    public MockSplitSource setBatchSize(int batchSize)
    {
        checkArgument(atSplitDepletion == DO_NOTHING, "cannot modify batch size once split completion action is set");
        this.batchSize = batchSize;
        return this;
    }

    public MockSplitSource increaseAvailableSplits(int count)
    {
        checkArgument(atSplitDepletion == DO_NOTHING, "cannot increase available splits once split completion action is set");
        totalSplits += count;
        doGetNextBatch();
        return this;
    }

    public MockSplitSource atSplitCompletion(Action action)
    {
        atSplitDepletion = action;
        doGetNextBatch();
        return this;
    }

    @Override
    public CatalogName getCatalogName()
    {
        throw new UnsupportedOperationException();
    }

    private void doGetNextBatch()
    {
        checkState(splitsProduced <= totalSplits);
        if (splitsProduced == totalSplits) {
            switch (atSplitDepletion) {
                case FAIL:
                    nextBatchFuture.setException(new IllegalStateException("Mock failure"));
                    break;
                case FINISH:
                    nextBatchFuture.set(ImmutableList.of());
                    break;
                case DO_NOTHING:
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        int splits = Math.min(Math.min(batchSize, nextBatchMaxSize), totalSplits - splitsProduced);
        if (splits != 0) {
            splitsProduced += splits;
            nextBatchFuture.set(Collections.nCopies(splits, SPLIT));
        }
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize)
    {
        if (partitionHandle != NOT_PARTITIONED) {
            throw new UnsupportedOperationException();
        }
        checkArgument(Lifespan.taskWide().equals(lifespan));

        checkState(nextBatchFuture.isDone(), "concurrent getNextBatch invocation");
        nextBatchFuture = SettableFuture.create();
        nextBatchMaxSize = maxSize;
        nextBatchInvocationCount++;
        doGetNextBatch();

        return Futures.transform(nextBatchFuture, splits -> new SplitBatch(splits, isFinished()), directExecutor());
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return splitsProduced == totalSplits && atSplitDepletion == FINISH;
    }

    @Override
    public Optional<Integer> getMinScheduleSplitBatchSize()
    {
        return Optional.empty();
    }

    public int getNextBatchInvocationCount()
    {
        return nextBatchInvocationCount;
    }

    public static class MockConnectorSplit
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return "A mock split";
        }
    }

    public enum Action
    {
        DO_NOTHING,
        FAIL,
        FINISH,
    }
}
