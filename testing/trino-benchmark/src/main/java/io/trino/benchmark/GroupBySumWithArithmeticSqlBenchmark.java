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

import io.trino.testing.LocalQueryRunner;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class GroupBySumWithArithmeticSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupBySumWithArithmeticSqlBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner,
                "sql_groupby_agg_with_arithmetic",
                1,
                4,
                "select linestatus, sum(orderkey - partkey) from lineitem group by linestatus");
    }

    public static void main(String[] args)
    {
        new GroupBySumWithArithmeticSqlBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
