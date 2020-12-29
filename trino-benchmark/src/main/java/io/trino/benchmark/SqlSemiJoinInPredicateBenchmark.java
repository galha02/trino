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

public class SqlSemiJoinInPredicateBenchmark
        extends AbstractSqlBenchmark
{
    public SqlSemiJoinInPredicateBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner,
                "sql_semijoin_in",
                2,
                4,
                "SELECT orderkey FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey % 2 = 0)");
    }

    public static void main(String[] args)
    {
        new SqlSemiJoinInPredicateBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
