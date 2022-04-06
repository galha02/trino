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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.aggregation.Aggregator;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.partial.PartialAggregationOutputProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<AggregatorFactory> aggregatorFactories;
        private final Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor;
        private boolean closed;

        public AggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, List<AggregatorFactory> aggregatorFactories)
        {
            this(operatorId, planNodeId, aggregatorFactories, Optional.empty());
        }

        public AggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<AggregatorFactory> aggregatorFactories,
                Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.aggregatorFactories = ImmutableList.copyOf(aggregatorFactories);
            this.partialAggregationOutputProcessor = requireNonNull(partialAggregationOutputProcessor, "partialAggregationOutputProcessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, aggregatorFactories, partialAggregationOutputProcessor);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOperatorFactory(operatorId, planNodeId, aggregatorFactories, partialAggregationOutputProcessor);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Aggregator> aggregates;
    private final Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor;

    private State state = State.NEEDS_INPUT;

    public AggregationOperator(
            OperatorContext operatorContext,
            List<AggregatorFactory> aggregatorFactories,
            Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.userMemoryContext = operatorContext.localUserMemoryContext();
        this.partialAggregationOutputProcessor = requireNonNull(partialAggregationOutputProcessor, "partialAggregationOutputProcessor is null");

        aggregates = aggregatorFactories.stream()
                .map(AggregatorFactory::createAggregator)
                .collect(toImmutableList());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public void close()
    {
        userMemoryContext.setBytes(0);
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        long memorySize = 0;
        for (Aggregator aggregate : aggregates) {
            aggregate.processPage(page);
            memorySize += aggregate.getEstimatedSize();
        }
        userMemoryContext.setBytes(memorySize);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        List<Type> types = aggregates.stream().map(Aggregator::getType).collect(toImmutableList());

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder pageBuilder = new PageBuilder(1, types);

        pageBuilder.declarePosition();
        for (int i = 0; i < aggregates.size(); i++) {
            Aggregator aggregator = aggregates.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            aggregator.evaluate(blockBuilder);
        }

        state = State.FINISHED;
        Page output = pageBuilder.build();
        return partialAggregationOutputProcessor
                .map(outputProcessor -> outputProcessor.processAggregatedPage(output))
                .orElse(output);
    }
}
