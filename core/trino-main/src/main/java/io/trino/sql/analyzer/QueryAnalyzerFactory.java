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
package io.trino.sql.analyzer;

import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanOptimizersFactory;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class QueryAnalyzerFactory
{
    private final PlanOptimizersFactory planOptimizersFactory;
    private final PlannerContext plannerContext;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    @Inject
    public QueryAnalyzerFactory(
            PlanOptimizersFactory planOptimizersFactory,
            PlannerContext plannerContext,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.planOptimizersFactory = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
        this.plannerContext = requireNonNull(plannerContext, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public QueryAnalyzer createQueryAnalyzer(AnalyzerFactory analyzerFactory, StatementAnalyzerFactory statementAnalyzerFactory)
    {
        return new QueryAnalyzer(
                this,
                planOptimizersFactory,
                plannerContext,
                analyzerFactory,
                statementAnalyzerFactory,
                statsCalculator,
                costCalculator);
    }
}
