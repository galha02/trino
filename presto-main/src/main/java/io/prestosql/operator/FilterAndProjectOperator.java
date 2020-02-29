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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.BasicWorkProcessorOperatorAdapter.BasicAdapterWorkProcessorOperatorFactory;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.BasicWorkProcessorOperatorAdapter.createAdapterOperatorFactory;
import static io.prestosql.operator.project.MergePages.mergePages;
import static java.util.Objects.requireNonNull;

public class FilterAndProjectOperator
        implements WorkProcessorOperator
{
    private final WorkProcessor<Page> pages;

    private FilterAndProjectOperator(
            Session session,
            MemoryTrackingContext memoryTrackingContext,
            DriverYieldSignal yieldSignal,
            WorkProcessor<Page> sourcePages,
            PageProcessor pageProcessor,
            List<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount,
            boolean avoidPageMaterialization)
    {
        AggregatedMemoryContext localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
        LocalMemoryContext outputMemoryContext = localAggregatedMemoryContext.newLocalMemoryContext(FilterAndProjectOperator.class.getSimpleName());

        this.pages = sourcePages
                .flatMap(page -> pageProcessor.createWorkProcessor(
                        session.toConnectorSession(),
                        yieldSignal,
                        outputMemoryContext,
                        page,
                        avoidPageMaterialization))
                .transformProcessor(processor -> mergePages(types, minOutputPageSize.toBytes(), minOutputPageRowCount, processor, localAggregatedMemoryContext))
                .withProcessStateMonitor(state -> memoryTrackingContext.localSystemMemoryContext().setBytes(localAggregatedMemoryContext.getBytes()));
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    public static OperatorFactory createOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            Supplier<PageProcessor> processor,
            List<Type> types,
            DataSize minOutputPageSize,
            int minOutputPageRowCount)
    {
        return createAdapterOperatorFactory(new Factory(
                operatorId,
                planNodeId,
                processor,
                types,
                minOutputPageSize,
                minOutputPageRowCount));
    }

    private static class Factory
            implements BasicAdapterWorkProcessorOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<PageProcessor> processor;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;

        private Factory(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<PageProcessor> processor,
                List<Type> types,
                DataSize minOutputPageSize,
                int minOutputPageRowCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new FilterAndProjectOperator(
                    processorContext.getSession(),
                    processorContext.getMemoryTrackingContext(),
                    processorContext.getDriverYieldSignal(),
                    sourcePages,
                    processor.get(),
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount,
                    true);
        }

        @Override
        public WorkProcessorOperator createAdapterOperator(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            checkState(!closed, "Factory is already closed");
            return new FilterAndProjectOperator(
                    processorContext.getSession(),
                    processorContext.getMemoryTrackingContext(),
                    processorContext.getDriverYieldSignal(),
                    sourcePages,
                    processor.get(),
                    types,
                    minOutputPageSize,
                    minOutputPageRowCount,
                    false);
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        @Override
        public String getOperatorType()
        {
            return FilterAndProjectOperator.class.getSimpleName();
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public BasicAdapterWorkProcessorOperatorFactory duplicate()
        {
            return new Factory(operatorId, planNodeId, processor, types, minOutputPageSize, minOutputPageRowCount);
        }
    }
}
