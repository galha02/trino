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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.RegularTableFunctionPartition.PassThroughColumnSpecification;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.TableFunctionProcessorProvider;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class TableFunctionOperator
        implements Operator
{
    public static class TableFunctionOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        // a provider of table function processor to be called once per partition
        private final TableFunctionProcessorProvider tableFunctionProvider;

        // all information necessary to execute the table function collected during analysis
        private final ConnectorTableFunctionHandle functionHandle;

        // number of proper columns produced by the table function
        private final int properChannelsCount;

        // number of input tables declared as pass-through
        private final int passThroughSourcesCount;

        // columns required by the table function, in order of input tables
        private final List<List<Integer>> requiredChannels;

        // map from input channel to marker channel
        // for each input table, there is a channel that marks which rows contain original data, and which are "filler" rows.
        // the "filler" rows are part of the algorithm, and they should not be processed by the table function, or passed-through.
        // In this map, every original column from the input table is associated with the appropriate marker.
        private final Optional<Map<Integer, Integer>> markerChannels;

        // necessary information to build a pass-through column, for all pass-through columns, ordered as expected on the output
        // it includes columns from sources declared as pass-through as well as partitioning columns from other sources
        private final List<PassThroughColumnSpecification> passThroughSpecifications;

        // specifies whether the function should be pruned or executed when the input is empty
        // pruneWhenEmpty is false if and only if all original input tables are KEEP WHEN EMPTY
        private final boolean pruneWhenEmpty;

        // partitioning channels from all sources
        private final List<Integer> partitionChannels;

        // subset of partition channels that are already grouped
        private final List<Integer> prePartitionedChannels;

        // channels necessary to sort all sources:
        // - for a single source, these are the source's sort channels
        // - for multiple sources, this is a single synthesized row number channel
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;

        // number of leading sort channels that are already sorted
        private final int preSortedPrefix;

        private final List<Type> sourceTypes;
        private final int expectedPositions;
        private final PagesIndex.Factory pagesIndexFactory;

        private boolean closed;

        public TableFunctionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFunctionProcessorProvider tableFunctionProvider,
                ConnectorTableFunctionHandle functionHandle,
                int properChannelsCount,
                int passThroughSourcesCount,
                List<List<Integer>> requiredChannels,
                Optional<Map<Integer, Integer>> markerChannels,
                List<PassThroughColumnSpecification> passThroughSpecifications,
                boolean pruneWhenEmpty,
                List<Integer> partitionChannels,
                List<Integer> prePartitionedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                int preSortedPrefix,
                List<? extends Type> sourceTypes,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory)
        {
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
            requireNonNull(functionHandle, "functionHandle is null");
            requireNonNull(requiredChannels, "requiredChannels is null");
            requireNonNull(markerChannels, "markerChannels is null");
            requireNonNull(passThroughSpecifications, "passThroughSpecifications is null");
            requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(prePartitionedChannels, "prePartitionedChannels is null");
            checkArgument(partitionChannels.containsAll(prePartitionedChannels), "prePartitionedChannels must be a subset of partitionChannels");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrders, "sortOrders is null");
            checkArgument(sortChannels.size() == sortOrders.size(), "The number of sort channels must be equal to the number of sort orders");
            checkArgument(preSortedPrefix <= sortChannels.size(), "The number of pre-sorted channels must be lower or equal to the number of sort channels");
            checkArgument(preSortedPrefix == 0 || ImmutableSet.copyOf(prePartitionedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedPrefix can only be greater than zero if all partition channels are pre-grouped");
            requireNonNull(sourceTypes, "sourceTypes is null");
            requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.tableFunctionProvider = tableFunctionProvider;
            this.functionHandle = functionHandle;
            this.properChannelsCount = properChannelsCount;
            this.passThroughSourcesCount = passThroughSourcesCount;
            this.requiredChannels = requiredChannels.stream()
                    .map(ImmutableList::copyOf)
                    .collect(toImmutableList());
            this.markerChannels = markerChannels.map(ImmutableMap::copyOf);
            this.passThroughSpecifications = ImmutableList.copyOf(passThroughSpecifications);
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.partitionChannels = ImmutableList.copyOf(partitionChannels);
            this.prePartitionedChannels = ImmutableList.copyOf(prePartitionedChannels);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrders = ImmutableList.copyOf(sortOrders);
            this.preSortedPrefix = preSortedPrefix;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.expectedPositions = expectedPositions;
            this.pagesIndexFactory = pagesIndexFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TableFunctionOperator.class.getSimpleName());
            return new TableFunctionOperator(
                    operatorContext,
                    tableFunctionProvider,
                    functionHandle,
                    properChannelsCount,
                    passThroughSourcesCount,
                    requiredChannels,
                    markerChannels,
                    passThroughSpecifications,
                    pruneWhenEmpty,
                    partitionChannels,
                    prePartitionedChannels,
                    sortChannels,
                    sortOrders,
                    preSortedPrefix,
                    sourceTypes,
                    expectedPositions,
                    pagesIndexFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFunctionOperatorFactory(
                    operatorId,
                    planNodeId,
                    tableFunctionProvider,
                    functionHandle,
                    properChannelsCount,
                    passThroughSourcesCount,
                    requiredChannels,
                    markerChannels,
                    passThroughSpecifications,
                    pruneWhenEmpty,
                    partitionChannels,
                    prePartitionedChannels,
                    sortChannels,
                    sortOrders,
                    preSortedPrefix,
                    sourceTypes,
                    expectedPositions,
                    pagesIndexFactory);
        }
    }

    private final OperatorContext operatorContext;

    private final PageBuffer pageBuffer = new PageBuffer();
    private final WorkProcessor<Page> outputPages;
    private final boolean processEmptyInput;

    public TableFunctionOperator(
            OperatorContext operatorContext,
            TableFunctionProcessorProvider tableFunctionProvider,
            ConnectorTableFunctionHandle functionHandle,
            int properChannelsCount,
            int passThroughSourcesCount,
            List<List<Integer>> requiredChannels,
            Optional<Map<Integer, Integer>> markerChannels,
            List<PassThroughColumnSpecification> passThroughSpecifications,
            boolean pruneWhenEmpty,
            List<Integer> partitionChannels,
            List<Integer> prePartitionedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            int preSortedPrefix,
            List<Type> sourceTypes,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory)
    {
        requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(tableFunctionProvider, "tableFunctionProvider is null");
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(requiredChannels, "requiredChannels is null");
        requireNonNull(markerChannels, "markerChannels is null");
        requireNonNull(passThroughSpecifications, "passThroughSpecifications is null");
        requireNonNull(partitionChannels, "partitionChannels is null");
        requireNonNull(prePartitionedChannels, "prePartitionedChannels is null");
        checkArgument(partitionChannels.containsAll(prePartitionedChannels), "prePartitionedChannels must be a subset of partitionChannels");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortChannels.size() == sortOrders.size(), "The number of sort channels must be equal to the number of sort orders");
        checkArgument(preSortedPrefix <= sortChannels.size(), "The number of pre-sorted channels must be lower or equal to the number of sort channels");
        checkArgument(preSortedPrefix == 0 || ImmutableSet.copyOf(prePartitionedChannels).equals(ImmutableSet.copyOf(partitionChannels)), "preSortedPrefix can only be greater than zero if all partition channels are pre-grouped");
        requireNonNull(sourceTypes, "sourceTypes is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;

        this.processEmptyInput = !pruneWhenEmpty;

        PagesIndex pagesIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
        HashStrategies hashStrategies = new HashStrategies(pagesIndex, partitionChannels, prePartitionedChannels, sortChannels, sortOrders, preSortedPrefix);

        this.outputPages = pageBuffer.pages()
                .transform(new PartitionAndSort(pagesIndex, hashStrategies, processEmptyInput))
                .flatMap(groupPagesIndex -> pagesIndexToTableFunctionPartitions(
                        groupPagesIndex,
                        hashStrategies,
                        tableFunctionProvider,
                        functionHandle,
                        properChannelsCount,
                        passThroughSourcesCount,
                        requiredChannels,
                        markerChannels,
                        passThroughSpecifications,
                        processEmptyInput))
                .flatMap(TableFunctionPartition::toOutputPages);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public boolean isFinished()
    {
        return outputPages.isFinished();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (outputPages.isBlocked()) {
            return outputPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public Page getOutput()
    {
        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            return null;
        }

        return outputPages.getResult();
    }

    private static class HashStrategies
    {
        final PagesHashStrategy prePartitionedStrategy;
        final PagesHashStrategy remainingPartitionStrategy;
        final PagesHashStrategy preSortedStrategy;
        final List<Integer> remainingPartitionAndSortChannels;
        final List<SortOrder> remainingSortOrders;
        final int[] prePartitionedChannelsArray;

        public HashStrategies(
                PagesIndex pagesIndex,
                List<Integer> partitionChannels,
                List<Integer> prePartitionedChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                int preSortedPrefix)
        {
            this.prePartitionedStrategy = pagesIndex.createPagesHashStrategy(prePartitionedChannels, OptionalInt.empty());

            List<Integer> remainingPartitionChannels = partitionChannels.stream()
                    .filter(channel -> !prePartitionedChannels.contains(channel))
                    .collect(toImmutableList());
            this.remainingPartitionStrategy = pagesIndex.createPagesHashStrategy(remainingPartitionChannels, OptionalInt.empty());

            List<Integer> preSortedChannels = sortChannels.stream()
                    .limit(preSortedPrefix)
                    .collect(toImmutableList());
            this.preSortedStrategy = pagesIndex.createPagesHashStrategy(preSortedChannels, OptionalInt.empty());

            if (preSortedPrefix > 0) {
                // preSortedPrefix > 0 implies that all partition channels are already pre-partitioned (enforced by check in the constructor), so we only need to do the remaining sort
                this.remainingPartitionAndSortChannels = ImmutableList.copyOf(Iterables.skip(sortChannels, preSortedPrefix));
                this.remainingSortOrders = ImmutableList.copyOf(Iterables.skip(sortOrders, preSortedPrefix));
            }
            else {
                // we need to sort by the remaining partition channels so that the input is fully partitioned,
                // and then need to we sort by all the sort channels so that the input is fully sorted
                this.remainingPartitionAndSortChannels = ImmutableList.copyOf(concat(remainingPartitionChannels, sortChannels));
                this.remainingSortOrders = ImmutableList.copyOf(concat(nCopies(remainingPartitionChannels.size(), ASC_NULLS_LAST), sortOrders));
            }

            this.prePartitionedChannelsArray = Ints.toArray(prePartitionedChannels);
        }
    }

    private class PartitionAndSort
            implements WorkProcessor.Transformation<Page, PagesIndex>
    {
        private final PagesIndex pagesIndex;
        private final HashStrategies hashStrategies;
        private final LocalMemoryContext memoryContext;

        private boolean resetPagesIndex;
        private int pendingInputPosition;
        private boolean processEmptyInput;

        public PartitionAndSort(PagesIndex pagesIndex, HashStrategies hashStrategies, boolean processEmptyInput)
        {
            this.pagesIndex = pagesIndex;
            this.hashStrategies = hashStrategies;
            this.memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext(PartitionAndSort.class.getSimpleName());
            this.processEmptyInput = processEmptyInput;
        }

        @Override
        public WorkProcessor.TransformationState<PagesIndex> process(Page pendingInput)
        {
            if (resetPagesIndex) {
                pagesIndex.clear();
                updateMemoryUsage();
                resetPagesIndex = false;
            }

            boolean finishing = pendingInput == null;
            if (finishing && pagesIndex.getPositionCount() == 0) {
                if (processEmptyInput) {
                    // it can only happen at the first call to process(), which implies that there is no input. Empty PagesIndex can be passed on only once.
                    processEmptyInput = false;
                    return WorkProcessor.TransformationState.ofResult(pagesIndex, false);
                }
                else {
                    memoryContext.close();
                    return WorkProcessor.TransformationState.finished();
                }
            }

            // there is input, so we are not interested in processing empty input
            processEmptyInput = false;

            if (!finishing) {
                // append rows from pendingInput which belong to the current group wrt pre-partitioned columns
                // it might be one or more partitions
                pendingInputPosition = appendCurrentGroup(pagesIndex, hashStrategies, pendingInput, pendingInputPosition);
                updateMemoryUsage();
            }

            // if we have unused input or are finishing, then we have buffered a full group
            if (finishing || pendingInputPosition < pendingInput.getPositionCount()) {
                // the group contains one or more partitions, as it was determined by the pre-partitioned columns
                // sorting serves two purposes:
                // - sort by the remaining partition channels so that the input is fully partitioned,
                // - sort by all the sort channels so that the input is fully sorted
                sortCurrentGroup(pagesIndex, hashStrategies);
                resetPagesIndex = true;
                return WorkProcessor.TransformationState.ofResult(pagesIndex, false);
            }

            pendingInputPosition = 0;
            return WorkProcessor.TransformationState.needsMoreData();
        }

        void updateMemoryUsage()
        {
            memoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
        }
    }

    private static int appendCurrentGroup(PagesIndex pagesIndex, HashStrategies hashStrategies, Page page, int startPosition)
    {
        checkArgument(page.getPositionCount() > startPosition);

        PagesHashStrategy prePartitionedStrategy = hashStrategies.prePartitionedStrategy;
        Page prePartitionedPage = page.getColumns(hashStrategies.prePartitionedChannelsArray);

        if (pagesIndex.getPositionCount() == 0 || pagesIndex.positionNotDistinctFromRow(prePartitionedStrategy, 0, startPosition, prePartitionedPage)) {
            // we are within the current group. find the position where the pre-grouped columns change
            int groupEnd = findGroupEnd(prePartitionedPage, prePartitionedStrategy, startPosition);

            // add the section of the page that contains values for the current group
            pagesIndex.addPage(page.getRegion(startPosition, groupEnd - startPosition));

            if (page.getPositionCount() - groupEnd > 0) {
                // the remaining prt of the page contains the next group
                return groupEnd;
            }
            // page fully consumed: it contains the current group only
            return page.getPositionCount();
        }

        // we had previous results buffered, but the remaining page starts with new group values
        return startPosition;
    }

    private static void sortCurrentGroup(PagesIndex pagesIndex, HashStrategies hashStrategies)
    {
        PagesHashStrategy preSortedStrategy = hashStrategies.preSortedStrategy;
        List<Integer> remainingPartitionAndSortChannels = hashStrategies.remainingPartitionAndSortChannels;
        List<SortOrder> remainingSortOrders = hashStrategies.remainingSortOrders;

        if (pagesIndex.getPositionCount() > 1 && !remainingPartitionAndSortChannels.isEmpty()) {
            int startPosition = 0;
            while (startPosition < pagesIndex.getPositionCount()) {
                int endPosition = findGroupEnd(pagesIndex, preSortedStrategy, startPosition);
                pagesIndex.sort(remainingPartitionAndSortChannels, remainingSortOrders, startPosition, endPosition);
                startPosition = endPosition;
            }
        }
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(Page page, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(page.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, page.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, page.getPositionCount(), (firstPosition, secondPosition) -> pagesHashStrategy.rowNotDistinctFromRow(firstPosition, page, secondPosition, page));
    }

    // Assumes input grouped on relevant pagesHashStrategy columns
    private static int findGroupEnd(PagesIndex pagesIndex, PagesHashStrategy pagesHashStrategy, int startPosition)
    {
        checkArgument(pagesIndex.getPositionCount() > 0, "Must have at least one position");
        checkPositionIndex(startPosition, pagesIndex.getPositionCount(), "startPosition out of bounds");

        return findEndPosition(startPosition, pagesIndex.getPositionCount(), (firstPosition, secondPosition) -> pagesIndex.positionNotDistinctFromPosition(pagesHashStrategy, firstPosition, secondPosition));
    }

    /**
     * @param startPosition - inclusive
     * @param endPosition - exclusive
     * @param comparator - returns true if positions given as parameters are equal
     * @return the end of the group position exclusive (the position the very next group starts)
     */
    @VisibleForTesting
    static int findEndPosition(int startPosition, int endPosition, PositionComparator comparator)
    {
        checkArgument(startPosition >= 0, "startPosition must be greater or equal than zero: %s", startPosition);
        checkArgument(startPosition < endPosition, "startPosition (%s) must be less than endPosition (%s)", startPosition, endPosition);

        int left = startPosition;
        int right = endPosition;

        while (right - left > 1) {
            int middle = (left + right) >>> 1;

            if (comparator.test(startPosition, middle)) {
                left = middle;
            }
            else {
                right = middle;
            }
        }

        return right;
    }

    private interface PositionComparator
    {
        boolean test(int first, int second);
    }

    private WorkProcessor<TableFunctionPartition> pagesIndexToTableFunctionPartitions(
            PagesIndex pagesIndex,
            HashStrategies hashStrategies,
            TableFunctionProcessorProvider tableFunctionProvider,
            ConnectorTableFunctionHandle functionHandle,
            int properChannelsCount,
            int passThroughSourcesCount,
            List<List<Integer>> requiredChannels,
            Optional<Map<Integer, Integer>> markerChannels,
            List<PassThroughColumnSpecification> passThroughSpecifications,
            boolean processEmptyInput)
    {
        // pagesIndex contains the full grouped and sorted data for one or more partitions

        PagesHashStrategy remainingPartitionStrategy = hashStrategies.remainingPartitionStrategy;

        return WorkProcessor.create(new WorkProcessor.Process<>()
        {
            private int partitionStart;
            private boolean processEmpty = processEmptyInput;

            @Override
            public WorkProcessor.ProcessState<TableFunctionPartition> process()
            {
                if (partitionStart == pagesIndex.getPositionCount()) {
                    if (processEmpty && pagesIndex.getPositionCount() == 0) {
                        // empty PagesIndex can only be passed once as the result of PartitionAndSort. Neither this nor any future instance of Process will ever get an empty PagesIndex again.
                        processEmpty = false;
                        return WorkProcessor.ProcessState.ofResult(new EmptyTableFunctionPartition(
                                tableFunctionProvider.get(functionHandle),
                                properChannelsCount,
                                passThroughSourcesCount,
                                passThroughSpecifications.stream()
                                        .map(PassThroughColumnSpecification::inputChannel)
                                        .map(pagesIndex::getType)
                                        .collect(toImmutableList())));
                    }
                    return WorkProcessor.ProcessState.finished();
                }

                // there is input, so we are not interested in processing empty input
                processEmpty = false;

                int partitionEnd = findGroupEnd(pagesIndex, remainingPartitionStrategy, partitionStart);

                RegularTableFunctionPartition partition = new RegularTableFunctionPartition(
                        pagesIndex,
                        partitionStart,
                        partitionEnd,
                        tableFunctionProvider.get(functionHandle),
                        properChannelsCount,
                        passThroughSourcesCount,
                        requiredChannels,
                        markerChannels,
                        passThroughSpecifications);

                partitionStart = partitionEnd;
                return WorkProcessor.ProcessState.ofResult(partition);
            }
        });
    }
}
