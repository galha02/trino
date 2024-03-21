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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.SampleNode.Type;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveFullSample
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveFullSample())
                .on(p ->
                        p.sample(
                                0.15,
                                Type.BERNOULLI,
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new RemoveFullSample())
                .on(p ->
                        p.sample(
                                1.0,
                                Type.BERNOULLI,
                                p.filter(
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 5L)),
                                        p.values(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                ImmutableList.of(
                                                        ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 10L)),
                                                        ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 11L)))))))
                // TODO: verify contents
                .matches(filter(
                        new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 5L)),
                        values(ImmutableMap.of("a", 0, "b", 1))));
    }
}
