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

package io.trino.sql.planner.iterative;

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.BooleanLiteral;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuleIndex
{
    private final PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);

    @Test
    public void testWithPlanNodeHierarchy()
    {
        Rule<?> projectRule1 = new NoOpRule<>(Pattern.typeOf(ProjectNode.class));
        Rule<?> projectRule2 = new NoOpRule<>(Pattern.typeOf(ProjectNode.class));
        Rule<?> filterRule = new NoOpRule<>(Pattern.typeOf(FilterNode.class));
        Rule<?> anyRule = new NoOpRule<>(Pattern.any());

        RuleIndex ruleIndex = RuleIndex.builder()
                .register(projectRule1)
                .register(projectRule2)
                .register(filterRule)
                .register(anyRule)
                .build();

        ProjectNode projectNode = planBuilder.project(Assignments.of(), planBuilder.values());
        FilterNode filterNode = planBuilder.filter(BooleanLiteral.TRUE_LITERAL, planBuilder.values());
        ValuesNode valuesNode = planBuilder.values();

        assertThat(ruleIndex.getCandidates(projectNode).collect(toSet())).isEqualTo(ImmutableSet.of(projectRule1, projectRule2, anyRule));
        assertThat(ruleIndex.getCandidates(filterNode).collect(toSet())).isEqualTo(ImmutableSet.of(filterRule, anyRule));
        assertThat(ruleIndex.getCandidates(valuesNode).collect(toSet())).isEqualTo(ImmutableSet.of(anyRule));
    }

    @Test
    public void testInterfacesHierarchy()
    {
        Rule<?> a = new NoOpRule<>(Pattern.typeOf(A.class));
        Rule<?> b = new NoOpRule<>(Pattern.typeOf(B.class));
        Rule<?> ab = new NoOpRule<>(Pattern.typeOf(AB.class));

        RuleIndex ruleIndex = RuleIndex.builder()
                .register(a)
                .register(b)
                .register(ab)
                .build();

        assertThat(ruleIndex.getCandidates(new A() { }).collect(toSet())).isEqualTo(ImmutableSet.of(a));
        assertThat(ruleIndex.getCandidates(new B() { }).collect(toSet())).isEqualTo(ImmutableSet.of(b));
        assertThat(ruleIndex.getCandidates(new AB()).collect(toSet())).isEqualTo(ImmutableSet.of(ab, a, b));
    }

    private static class NoOpRule<T>
            implements Rule<T>
    {
        private final Pattern<T> pattern;

        private NoOpRule(Pattern<T> pattern)
        {
            this.pattern = pattern;
        }

        @Override
        public Pattern<T> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(T node, Captures captures, Context context)
        {
            return Result.empty();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pattern", pattern)
                    .toString();
        }
    }

    private interface A {}

    private interface B {}

    private static class AB
            implements A, B {}
}
