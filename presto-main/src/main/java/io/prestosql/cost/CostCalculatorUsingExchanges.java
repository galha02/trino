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

package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.GroupReference;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateLocalRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteGatherCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteReplicateCost;
import static io.prestosql.cost.LocalCostEstimate.addPartialComponents;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorUsingExchanges(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, Session session, TypeProvider types)
    {
        CostEstimator costEstimator = new CostEstimator(stats, types, taskCountEstimator);
        return node.accept(costEstimator, null).toPlanCost();
    }

    private static class CostEstimator
            extends PlanVisitor<LocalCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final TypeProvider types;
        private final TaskCountEstimator taskCountEstimator;

        CostEstimator(StatsProvider stats, TypeProvider types, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.types = requireNonNull(types, "types is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        protected LocalCostEstimate visitPlan(PlanNode node, Void context)
        {
            return LocalCostEstimate.unknown();
        }

        @Override
        public LocalCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdColumn()), types));
        }

        @Override
        public LocalCostEstimate visitRowNumber(RowNumberNode node, Void context)
        {
            List<Symbol> symbols = node.getOutputSymbols();
            // when maxRowCountPerPartition is set, the RowNumberOperator
            // copies values for all the columns into a page builder
            if (!node.getMaxRowCountPerPartition().isPresent()) {
                symbols = ImmutableList.<Symbol>builder()
                        .addAll(node.getPartitionBy())
                        .add(node.getRowNumberSymbol())
                        .build();
            }
            PlanNodeStatsEstimate stats = getStats(node);
            double cpuCost = stats.getOutputSizeInBytes(symbols, types);
            double memoryCost = node.getPartitionBy().isEmpty() ? 0 : stats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            return LocalCostEstimate.of(cpuCost, memoryCost, 0);
        }

        @Override
        public LocalCostEstimate visitOutput(OutputNode node, Void context)
        {
            return LocalCostEstimate.zero();
        }

        @Override
        public LocalCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            return LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public LocalCostEstimate visitFilter(FilterNode node, Void context)
        {
            return LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public LocalCostEstimate visitProject(ProjectNode node, Void context)
        {
            return LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public LocalCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() != FINAL && node.getStep() != SINGLE) {
                return LocalCostEstimate.unknown();
            }
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputSymbols(), types);
            return LocalCostEstimate.of(cpuCost, memoryCost, 0);
        }

        @Override
        public LocalCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
        }

        private LocalCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            LocalCostEstimate joinInputCost = calculateJoinInputCost(
                    probe,
                    build,
                    stats,
                    types,
                    replicated,
                    taskCountEstimator.estimateSourceDistributedTaskCount());
            LocalCostEstimate joinOutputCost = calculateJoinOutputCost(join);
            return addPartialComponents(joinInputCost, joinOutputCost);
        }

        private LocalCostEstimate calculateJoinOutputCost(PlanNode join)
        {
            PlanNodeStatsEstimate outputStats = getStats(join);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputSymbols(), types);
            return LocalCostEstimate.ofCpu(joinOutputSize);
        }

        @Override
        public LocalCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types);
            switch (node.getScope()) {
                case LOCAL:
                    switch (node.getType()) {
                        case GATHER:
                            return LocalCostEstimate.zero();
                        case REPARTITION:
                            return calculateLocalRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            return LocalCostEstimate.zero();
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                case REMOTE:
                    switch (node.getType()) {
                        case GATHER:
                            return calculateRemoteGatherCost(inputSizeInBytes);
                        case REPARTITION:
                            return calculateRemoteRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            // assuming that destination is always source distributed
                            // it is true as now replicated exchange is used for joins only
                            // for replicated join probe side is usually source distributed
                            return calculateRemoteReplicateCost(inputSizeInBytes, taskCountEstimator.estimateSourceDistributedTaskCount());
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                default:
                    throw new IllegalArgumentException("Unexpected scope: " + node.getScope());
            }
        }

        @Override
        public LocalCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
        }

        @Override
        public LocalCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED);
        }

        @Override
        public LocalCostEstimate visitValues(ValuesNode node, Void context)
        {
            return LocalCostEstimate.zero();
        }

        @Override
        public LocalCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return LocalCostEstimate.zero();
        }

        @Override
        public LocalCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            return LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
        }

        @Override
        public LocalCostEstimate visitUnion(UnionNode node, Void context)
        {
            // Cost will be accounted either in CostCalculatorUsingExchanges#CostEstimator#visitExchanged
            // or in CostCalculatorWithEstimatedExchanges#CostEstimator#visitUnion
            // This stub is needed just to avoid the cumulative cost being set to unknown
            return LocalCostEstimate.zero();
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }
    }
}
