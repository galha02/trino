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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;

public class TestAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testAggregationWhenAllStatisticsAreKnown()
    {
        Consumer<PlanNodeStatsAssertion> outputRowCountAndZStatsAreCalculated = check -> check
                .outputRowsCount(15)
                .symbolStats("z", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCount(4)
                        .nullsFraction(0.2))
                .symbolStats("y", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(0)
                        .highValue(3)
                        .distinctValuesCount(3)
                        .nullsFraction(0));

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        Consumer<PlanNodeStatsAssertion> outputRowsCountAndZStatsAreNotFullyCalculated = check -> check
                .outputRowsCountUnknown()
                .symbolStats("z", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCountUnknown()
                        .nullsFractionUnknown())
                .symbolStats("y", symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(0)
                        .highValue(3)
                        .distinctValuesCount(3)
                        .nullsFraction(0));

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);

        testAggregation(
                SymbolStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);
    }

    private StatsCalculatorAssertion testAggregation(SymbolStatsEstimate zStats)
    {
        return tester().assertStatsFor(pb -> pb
                .aggregation(ab -> ab
                        .addAggregation(pb.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                        .addAggregation(pb.symbol("count", BIGINT), aggregation("count", ImmutableList.of()), ImmutableList.of())
                        .addAggregation(pb.symbol("count_on_x", BIGINT), aggregation("count", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("x"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("z"), zStats)
                        .build())
                .check(check -> check
                        .symbolStats("sum", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("count", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("count_on_x", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .symbolStats("x", symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    @Test
    public void testAggregationStatsCappedToInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .aggregation(ab -> ab
                        .addAggregation(pb.symbol("count_on_x", BIGINT), aggregation("count", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                        .singleGroupingSet(pb.symbol("y", BIGINT), pb.symbol("z", BIGINT))
                        .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addSymbolStatistics(new Symbol("z"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testAggregationWithGlobalGrouping()
    {
        tester().assertStatsFor(pb -> pb
                        .aggregation(ab -> ab
                                .addAggregation(pb.symbol("count_on_x", BIGINT), aggregation("count", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                                .addAggregation(pb.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                                .globalGrouping()
                                .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.unknown())
                .check(check -> check.outputRowsCount(1));
    }

    @Test
    public void testAggregationWithMoreGroupingSets()
    {
        tester().assertStatsFor(pb -> pb
                        .aggregation(ab -> ab
                                .addAggregation(pb.symbol("count_on_x", BIGINT), aggregation("count", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                                .addAggregation(pb.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new SymbolReference("x"))), ImmutableList.of(BIGINT))
                                .groupingSets(new AggregationNode.GroupingSetDescriptor(ImmutableList.of(pb.symbol("y"), pb.symbol("z")), 3, ImmutableSet.of(0)))
                                .source(pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT), pb.symbol("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addSymbolStatistics(new Symbol("z"), SymbolStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCountUnknown());
    }
}
