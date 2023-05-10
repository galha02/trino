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
package io.trino.cache;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CacheDataOperator
        implements Operator
{
    public static class CacheDataOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final long maxSplitSizeInBytes;

        public CacheDataOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                long maxSplitSizeInBytes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.maxSplitSizeInBytes = maxSplitSizeInBytes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkArgument(driverContext.getCacheDriverContext().isPresent(), "cacheDriverContext is empty");
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, CacheDataOperator.class.getSimpleName());
            return new CacheDataOperator(operatorContext, maxSplitSizeInBytes, driverContext.getCacheDriverContext().get().cacheMetrics());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CacheDataOperatorFactory(operatorId, planNodeId, maxSplitSizeInBytes);
        }
    }

    private final OperatorContext operatorContext;
    private final CacheMetrics cacheMetrics;
    private final LocalMemoryContext memoryContext;
    private final long maxCacheSizeInBytes;

    @Nullable
    private ConnectorPageSink pageSink;
    @Nullable
    private Page page;
    private boolean isCachingAborted;

    private CacheDataOperator(OperatorContext operatorContext, long maxCacheSizeInBytes, CacheMetrics cacheMetrics)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.memoryContext = operatorContext.newLocalUserMemoryContext(CacheDataOperator.class.getSimpleName());
        this.pageSink = operatorContext.getDriverContext().getCacheDriverContext()
                .orElseThrow(() -> new IllegalArgumentException("Cache context is not present"))
                .pageSink()
                .orElseThrow(() -> new IllegalArgumentException("Cache page sink is not present"));
        memoryContext.setBytes(pageSink.getMemoryUsage());
        this.maxCacheSizeInBytes = maxCacheSizeInBytes;
        this.cacheMetrics = requireNonNull(cacheMetrics, "cacheMetrics is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return pageSink != null && page == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());
        this.page = page;

        if (isCachingAborted) {
            return;
        }

        checkState(pageSink.appendPage(page).isDone(), "appendPage future must be done");
        memoryContext.setBytes(pageSink.getMemoryUsage());

        // If there is no space for a page in a cache, stop caching this split and abort pageSink
        if (pageSink.getMemoryUsage() > maxCacheSizeInBytes) {
            pageSink.abort();
            isCachingAborted = true;
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = this.page;
        this.page = null;
        return page;
    }

    @Override
    public void finish()
    {
        if (pageSink != null) {
            if (isCachingAborted) {
                cacheMetrics.incrementSplitsNotCached();
            }
            else {
                checkState(pageSink.finish().isDone(), "finish future must be done");
                cacheMetrics.incrementSplitsCached();
            }
            pageSink = null;
            memoryContext.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        return pageSink == null && page == null;
    }

    @Override
    public void close()
            throws Exception
    {
        if (pageSink != null) {
            if (!isCachingAborted) {
                pageSink.abort();
            }
            pageSink = null;
            memoryContext.close();
        }
    }
}
