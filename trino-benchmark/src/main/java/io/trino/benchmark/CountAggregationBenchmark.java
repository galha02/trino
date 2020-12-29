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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.AggregationOperator.AggregationOperatorFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class CountAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public CountAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "count_agg", 10, 100);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey");
        Metadata metadata = localQueryRunner.getMetadata();
        InternalAggregationFunction countFunction = metadata.getAggregateFunctionImplementation(metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of()));
        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(1, new PlanNodeId("test"), Step.SINGLE, ImmutableList.of(countFunction.bind(ImmutableList.of(0), Optional.empty())), false);
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
