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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.SqlQueryExecution;
import io.prestosql.execution.StageState;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.predicate.DiscreteValues;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.operator.JoinUtils.isBuildSideRepartitioned;
import static io.prestosql.operator.JoinUtils.isBuildSideReplicated;
import static io.prestosql.spi.connector.DynamicFilter.EMPTY;
import static io.prestosql.spi.predicate.Domain.union;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

@ThreadSafe
public class DynamicFilterService
{
    private final Duration dynamicFilteringRefreshInterval;
    private final ScheduledExecutorService collectDynamicFiltersExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DynamicFilterService"));

    @GuardedBy("this") // for updates
    private final Map<QueryId, Map<DynamicFilterId, SettableFuture<Domain>>> dynamicFilterSummaries = new ConcurrentHashMap<>();
    @GuardedBy("this") // for updates
    private final Map<QueryId, Supplier<List<StageDynamicFilters>>> dynamicFilterSuppliers = new ConcurrentHashMap<>();
    @GuardedBy("this") // for updates
    private final Map<QueryId, Set<DynamicFilterId>> queryRepartitionedDynamicFilters = new ConcurrentHashMap<>();
    @GuardedBy("this") // for updates
    private final Map<QueryId, Set<DynamicFilterId>> queryReplicatedDynamicFilters = new ConcurrentHashMap<>();

    @Inject
    public DynamicFilterService(FeaturesConfig featuresConfig)
    {
        this.dynamicFilteringRefreshInterval = requireNonNull(featuresConfig, "featuresConfig is null").getDynamicFilteringRefreshInterval();
    }

    @PostConstruct
    public void start()
    {
        collectDynamicFiltersExecutor.scheduleWithFixedDelay(this::collectDynamicFilters, 0, dynamicFilteringRefreshInterval.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        collectDynamicFiltersExecutor.shutdownNow();
    }

    public void registerQuery(SqlQueryExecution sqlQueryExecution)
    {
        // register query only if it contains dynamic filters
        ImmutableSet.Builder<DynamicFilterId> dynamicFiltersBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DynamicFilterId> repartitionedDynamicFiltersBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DynamicFilterId> replicatedDynamicFiltersBuilder = ImmutableSet.builder();
        PlanNodeSearcher.searchFrom(sqlQueryExecution.getQueryPlan().getRoot())
                .where(JoinNode.class::isInstance)
                .<JoinNode>findAll()
                .forEach(node -> {
                    node.getDynamicFilters().keySet()
                            .forEach(dynamicFiltersBuilder::add);
                    if (isBuildSideReplicated(node)) {
                        replicatedDynamicFiltersBuilder.addAll(node.getDynamicFilters().keySet());
                    }
                    if (isBuildSideRepartitioned(node)) {
                        repartitionedDynamicFiltersBuilder.addAll(node.getDynamicFilters().keySet());
                    }
                });
        Set<DynamicFilterId> dynamicFilters = dynamicFiltersBuilder.build();
        if (!dynamicFilters.isEmpty()) {
            registerQuery(
                    sqlQueryExecution.getQueryId(),
                    sqlQueryExecution::getStageDynamicFilters,
                    dynamicFilters,
                    repartitionedDynamicFiltersBuilder.build(),
                    replicatedDynamicFiltersBuilder.build());
        }
    }

    @VisibleForTesting
    void registerQuery(
            QueryId queryId,
            Supplier<List<StageDynamicFilters>> stageDynamicFiltersSupplier,
            Set<DynamicFilterId> dynamicFilters,
            Set<DynamicFilterId> repartitionedDynamicFilters,
            Set<DynamicFilterId> replicatedDynamicFilters)
    {
        Map<DynamicFilterId, SettableFuture<Domain>> dynamicFilterFutures = dynamicFilters.stream()
                .collect(toImmutableMap(filter -> filter, filter -> SettableFuture.create()));
        synchronized (this) {
            if (dynamicFilterSummaries.containsKey(queryId)) {
                // query is already registered
                return;
            }

            dynamicFilterSummaries.put(queryId, dynamicFilterFutures);
            dynamicFilterSuppliers.put(queryId, stageDynamicFiltersSupplier);
            queryRepartitionedDynamicFilters.put(queryId, repartitionedDynamicFilters);
            queryReplicatedDynamicFilters.put(queryId, replicatedDynamicFilters);
        }
    }

    public DynamicFiltersStats getDynamicFilteringStats(QueryId queryId, Session session)
    {
        Map<DynamicFilterId, SettableFuture<Domain>> dynamicFilterFutures = dynamicFilterSummaries.getOrDefault(queryId, ImmutableMap.of());
        int numRepartitionedFilters = queryRepartitionedDynamicFilters.getOrDefault(queryId, ImmutableSet.of()).size();
        int numReplicatedFilters = queryReplicatedDynamicFilters.getOrDefault(queryId, ImmutableSet.of()).size();
        int numTotalDynamicFilters = dynamicFilterFutures.size();

        List<DynamicFilterDomainStats> dynamicFilterDomainStats = dynamicFilterFutures.entrySet().stream()
                .filter(entry -> entry.getValue().isDone())
                .map(entry -> {
                    DynamicFilterId dynamicFilterId = entry.getKey();
                    Domain domain = getDone(entry.getValue());
                    // simplify for readability
                    String simplifiedDomain = domain.simplify(1).toString(session.toConnectorSession());
                    int rangeCount = domain.getValues().getValuesProcessor().transform(
                            Ranges::getRangeCount,
                            discreteValues -> 0,
                            allOrNone -> 0);
                    int discreteValuesCount = domain.getValues().getValuesProcessor().transform(
                            ranges -> 0,
                            DiscreteValues::getValuesCount,
                            allOrNone -> 0);
                    return new DynamicFilterDomainStats(dynamicFilterId, simplifiedDomain, rangeCount, discreteValuesCount);
                })
                .collect(toImmutableList());
        return new DynamicFiltersStats(
                dynamicFilterDomainStats,
                numRepartitionedFilters,
                numReplicatedFilters,
                numTotalDynamicFilters,
                dynamicFilterDomainStats.size());
    }

    public synchronized void removeQuery(QueryId queryId)
    {
        dynamicFilterSummaries.remove(queryId);
        dynamicFilterSuppliers.remove(queryId);
        queryRepartitionedDynamicFilters.remove(queryId);
        queryReplicatedDynamicFilters.remove(queryId);
    }

    public DynamicFilter createDynamicFilter(QueryId queryId, List<DynamicFilters.Descriptor> dynamicFilterDescriptors, Map<Symbol, ColumnHandle> columnHandles)
    {
        Map<DynamicFilterId, ColumnHandle> sourceColumnHandles = extractSourceColumnHandles(dynamicFilterDescriptors, columnHandles);
        Set<DynamicFilterId> dynamicFilters = dynamicFilterDescriptors.stream()
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
        Map<DynamicFilterId, SettableFuture<Domain>> dynamicFilterFutures = dynamicFilterSummaries.get(queryId);

        if (dynamicFilterFutures == null) {
            // query has been removed
            return EMPTY;
        }

        List<ListenableFuture<?>> futures = dynamicFilters.stream()
                .map(dynamicFilterFutures::get)
                .collect(toImmutableList());
        List<ListenableFuture<?>> repartitionedFutures = dynamicFilters.stream()
                .filter(queryRepartitionedDynamicFilters.getOrDefault(queryId, ImmutableSet.of())::contains)
                .map(dynamicFilterFutures::get)
                .collect(toImmutableList());
        AtomicReference<TupleDomain<ColumnHandle>> completedDynamicFilter = new AtomicReference<>();

        return new DynamicFilter()
        {
            @Override
            public CompletableFuture<?> isBlocked()
            {
                // wait for any of the requested dynamic filter domains to be completed
                List<ListenableFuture<?>> undoneFutures = repartitionedFutures.stream()
                        .filter(future -> !future.isDone())
                        .collect(toImmutableList());

                if (undoneFutures.isEmpty()) {
                    return NOT_BLOCKED;
                }

                return toCompletableFuture(whenAnyComplete(undoneFutures));
            }

            @Override
            public boolean isComplete()
            {
                return futures.stream()
                        .allMatch(Future::isDone);
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                TupleDomain<ColumnHandle> dynamicFilter = completedDynamicFilter.get();
                if (dynamicFilter != null) {
                    return dynamicFilter;
                }

                dynamicFilter = dynamicFilters.stream()
                        .map(filter -> tryGetFutureValue(dynamicFilterFutures.get(filter))
                                .map(summary -> translateSummaryToTupleDomain(filter, summary, sourceColumnHandles)))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .reduce(TupleDomain.all(), TupleDomain::intersect);

                if (isComplete()) {
                    completedDynamicFilter.set(dynamicFilter);
                }

                return dynamicFilter;
            }
        };
    }

    @VisibleForTesting
    void collectDynamicFilters()
    {
        for (Map.Entry<QueryId, Supplier<List<StageDynamicFilters>>> entry : getDynamicFilterSuppliers().entrySet()) {
            QueryId queryId = entry.getKey();
            Set<DynamicFilterId> replicatedDynamicFilters = queryReplicatedDynamicFilters.getOrDefault(queryId, ImmutableSet.of());
            Set<DynamicFilterId> uncollectedFilters = getUncollectedDynamicFilters(queryId);
            ImmutableMap.Builder<DynamicFilterId, Domain> newDynamicFiltersBuilder = ImmutableMap.builder();
            for (StageDynamicFilters stageDynamicFilters : entry.getValue().get()) {
                StageState stageState = stageDynamicFilters.getStageState();
                stageDynamicFilters.getTaskDynamicFilters().stream()
                        .flatMap(taskDomains -> taskDomains.entrySet().stream())
                        .filter(domain -> uncollectedFilters.contains(domain.getKey()))
                        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toImmutableList())))
                        .entrySet().stream()
                        .filter(stageDomains -> {
                            if (replicatedDynamicFilters.contains(stageDomains.getKey())) {
                                // for replicated dynamic filters it's enough to get dynamic filter from a single task
                                return true;
                            }

                            // check if all tasks of a repartitioned dynamic filter source have reported dynamic filter summary
                            return !stageState.canScheduleMoreTasks() && stageDomains.getValue().size() == stageDynamicFilters.getNumberOfTasks();
                        })
                        .forEach(stageDomains -> newDynamicFiltersBuilder.put(stageDomains.getKey(), union(stageDomains.getValue())));
            }

            Map<DynamicFilterId, Domain> newDynamicFilters = newDynamicFiltersBuilder.build();
            if (!newDynamicFilters.isEmpty()) {
                addDynamicFilters(queryId, newDynamicFilters);
            }
        }
    }

    @VisibleForTesting
    Optional<Domain> getSummary(QueryId queryId, DynamicFilterId filterId)
    {
        return tryGetFutureValue(dynamicFilterSummaries.get(queryId).get(filterId));
    }

    private Map<QueryId, Supplier<List<StageDynamicFilters>>> getDynamicFilterSuppliers()
    {
        return ImmutableMap.copyOf(dynamicFilterSuppliers);
    }

    private synchronized void addDynamicFilters(QueryId queryId, Map<DynamicFilterId, Domain> dynamicFilters)
    {
        Map<DynamicFilterId, SettableFuture<Domain>> dynamicFilterFutures = dynamicFilterSummaries.get(queryId);
        if (dynamicFilterFutures == null) {
            // query might have been removed while filters were collected asynchronously
            return;
        }

        dynamicFilters.forEach((filter, domain) -> {
            SettableFuture<Domain> future = requireNonNull(dynamicFilterFutures.get(filter), "Future not found");
            checkState(future.set(domain), "Same future set twice");
        });

        // stop collecting dynamic filters for query when all dynamic filters have been collected
        boolean allCollected = dynamicFilterFutures.values().stream()
                .allMatch(future -> future.isDone());
        if (allCollected) {
            dynamicFilterSuppliers.remove(queryId);
        }
    }

    private Set<DynamicFilterId> getUncollectedDynamicFilters(QueryId queryId)
    {
        return dynamicFilterSummaries.getOrDefault(queryId, ImmutableMap.of()).entrySet().stream()
                .filter(entry -> !entry.getValue().isDone())
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());
    }

    private static TupleDomain<ColumnHandle> translateSummaryToTupleDomain(DynamicFilterId filterId, Domain summary, Map<DynamicFilterId, ColumnHandle> sourceColumnHandles)
    {
        ColumnHandle sourceColumnHandle = requireNonNull(sourceColumnHandles.get(filterId), () -> format("Source column handle for dynamic filter %s is null", filterId));
        return TupleDomain.withColumnDomains(ImmutableMap.of(sourceColumnHandle, summary));
    }

    private static Map<DynamicFilterId, ColumnHandle> extractSourceColumnHandles(List<DynamicFilters.Descriptor> dynamicFilters, Map<Symbol, ColumnHandle> columnHandles)
    {
        return dynamicFilters.stream()
                .collect(toImmutableMap(
                        DynamicFilters.Descriptor::getId,
                        descriptor -> columnHandles.get(Symbol.from(descriptor.getInput()))));
    }

    public static class StageDynamicFilters
    {
        private final StageState stageState;
        private final int numberOfTasks;
        private final List<Map<DynamicFilterId, Domain>> taskDynamicFilters;

        public StageDynamicFilters(StageState stageState, int numberOfTasks, List<Map<DynamicFilterId, Domain>> taskDynamicFilters)
        {
            this.stageState = requireNonNull(stageState, "stageState is null");
            this.numberOfTasks = numberOfTasks;
            this.taskDynamicFilters = ImmutableList.copyOf(requireNonNull(taskDynamicFilters, "taskDynamicFilters is null"));
        }

        private StageState getStageState()
        {
            return stageState;
        }

        private int getNumberOfTasks()
        {
            return numberOfTasks;
        }

        private List<Map<DynamicFilterId, Domain>> getTaskDynamicFilters()
        {
            return taskDynamicFilters;
        }
    }

    public static class DynamicFiltersStats
    {
        public static final DynamicFiltersStats EMPTY = new DynamicFiltersStats(ImmutableList.of(), 0, 0, 0, 0);

        private final List<DynamicFilterDomainStats> dynamicFilterDomainStats;
        private final int numRepartitionedDynamicFilters;
        private final int numReplicatedDynamicFilters;
        private final int numTotalDynamicFilters;
        private final int numDynamicFiltersCompleted;

        @JsonCreator
        public DynamicFiltersStats(
                @JsonProperty("dynamicFilterDomainStats") List<DynamicFilterDomainStats> dynamicFilterDomainStats,
                @JsonProperty("numRepartitionedDynamicFilters") int numRepartitionedDynamicFilters,
                @JsonProperty("numReplicatedDynamicFilters") int numReplicatedDynamicFilters,
                @JsonProperty("numTotalDynamicFilters") int numTotalDynamicFilters,
                @JsonProperty("numDynamicFiltersCompleted") int numDynamicFiltersCompleted)
        {
            this.dynamicFilterDomainStats = dynamicFilterDomainStats;
            this.numRepartitionedDynamicFilters = numRepartitionedDynamicFilters;
            this.numReplicatedDynamicFilters = numReplicatedDynamicFilters;
            this.numTotalDynamicFilters = numTotalDynamicFilters;
            this.numDynamicFiltersCompleted = numDynamicFiltersCompleted;
        }

        @JsonProperty
        public List<DynamicFilterDomainStats> getDynamicFilterDomainStats()
        {
            return dynamicFilterDomainStats;
        }

        @JsonProperty
        public int getNumRepartitionedDynamicFilters()
        {
            return numRepartitionedDynamicFilters;
        }

        @JsonProperty
        public int getNumReplicatedDynamicFilters()
        {
            return numReplicatedDynamicFilters;
        }

        @JsonProperty
        public int getNumTotalDynamicFilters()
        {
            return numTotalDynamicFilters;
        }

        @JsonProperty
        public int getNumDynamicFiltersCompleted()
        {
            return numDynamicFiltersCompleted;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DynamicFiltersStats that = (DynamicFiltersStats) o;
            return numRepartitionedDynamicFilters == that.numRepartitionedDynamicFilters &&
                    numReplicatedDynamicFilters == that.numReplicatedDynamicFilters &&
                    numTotalDynamicFilters == that.numTotalDynamicFilters &&
                    numDynamicFiltersCompleted == that.numDynamicFiltersCompleted &&
                    Objects.equals(dynamicFilterDomainStats, that.dynamicFilterDomainStats);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dynamicFilterDomainStats, numRepartitionedDynamicFilters, numReplicatedDynamicFilters, numTotalDynamicFilters, numDynamicFiltersCompleted);
        }
    }

    public static class DynamicFilterDomainStats
    {
        private final DynamicFilterId dynamicFilterId;
        private final String simplifiedDomain;
        private final int rangeCount;
        private final int discreteValuesCount;

        @JsonCreator
        public DynamicFilterDomainStats(
                @JsonProperty("dynamicFilterId") DynamicFilterId dynamicFilterId,
                @JsonProperty("simplifiedDomain") String simplifiedDomain,
                @JsonProperty("rangeCount") int rangeCount,
                @JsonProperty("discreteValuesCount") int discreteValuesCount)
        {
            this.dynamicFilterId = dynamicFilterId;
            this.simplifiedDomain = simplifiedDomain;
            this.rangeCount = rangeCount;
            this.discreteValuesCount = discreteValuesCount;
        }

        @JsonProperty
        public DynamicFilterId getDynamicFilterId()
        {
            return dynamicFilterId;
        }

        @JsonProperty
        public String getSimplifiedDomain()
        {
            return simplifiedDomain;
        }

        @JsonProperty
        public int getRangeCount()
        {
            return rangeCount;
        }

        @JsonProperty
        public int getDiscreteValuesCount()
        {
            return discreteValuesCount;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DynamicFilterDomainStats that = (DynamicFilterDomainStats) o;
            return rangeCount == that.rangeCount &&
                    discreteValuesCount == that.discreteValuesCount &&
                    Objects.equals(dynamicFilterId, that.dynamicFilterId) &&
                    Objects.equals(simplifiedDomain, that.simplifiedDomain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dynamicFilterId, simplifiedDomain, rangeCount, discreteValuesCount);
        }
    }
}
