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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.TaskCountEstimator;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.rule.DetermineTableScanNodePartitioning;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;

public class TestEliminateSorts
        extends BasePlanTest
{
    private static final String QUANTITY_ALIAS = "QUANTITY";

    private static final ExpectedValueProvider<WindowNode.Specification> windowSpec = specification(
            ImmutableList.of(),
            ImmutableList.of(QUANTITY_ALIAS),
            ImmutableMap.of(QUANTITY_ALIAS, SortOrder.ASC_NULLS_LAST));

    private static final PlanMatchPattern LINEITEM_TABLESCAN_Q = tableScan(
            "lineitem",
            ImmutableMap.of(QUANTITY_ALIAS, "quantity"));

    @Test
    public void testEliminateSorts()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY quantity";

        PlanMatchPattern pattern =
                output(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(windowSpec)
                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                anyTree(LINEITEM_TABLESCAN_Q)));

        assertUnitPlan(sql, pattern);
    }

    @Test
    public void testNotEliminateSorts()
    {
        @Language("SQL") String sql = "SELECT quantity, row_number() OVER (ORDER BY quantity) FROM lineitem ORDER BY tax";

        PlanMatchPattern pattern =
                anyTree(
                        sort(
                                anyTree(
                                        window(windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(windowSpec)
                                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                                anyTree(LINEITEM_TABLESCAN_Q)))));

        assertUnitPlan(sql, pattern);
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(new SqlParser(), getQueryRunner().getMetadata());
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new IterativeOptimizer(
                        getQueryRunner().getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new DetermineTableScanNodePartitioning(getQueryRunner().getMetadata(), getQueryRunner().getNodePartitioningManager(), new TaskCountEstimator(() -> 10)))),
                new AddExchanges(getQueryRunner().getMetadata(), getQueryRunner().getTypeOperators(), typeAnalyzer));

        assertPlan(sql, pattern, optimizers);
    }
}
