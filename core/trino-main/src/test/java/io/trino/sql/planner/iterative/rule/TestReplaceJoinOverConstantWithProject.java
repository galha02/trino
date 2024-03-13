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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestReplaceJoinOverConstantWithProject
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnJoinWithEmptySource()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(0, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0, p.symbol("a")),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnJoinWithCondition()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(5, p.symbol("b")),
                                new EquiJoinClause(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1, p.symbol("a")),
                                p.values(5, p.symbol("b")),
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new SymbolReference("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithMultipleRows()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(5, p.symbol("a")),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithNoOutputs()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnValuesWithNonRowExpression()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a")), ImmutableList.of(new Cast(new Row(ImmutableList.of(new StringLiteral("true"))), dataType("row(\"b\" boolean)")))),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnOuterJoinWhenSourcePossiblyEmpty()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(1, p.symbol("a")),
                                p.filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("5")),
                                        p.values(10, p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                RIGHT,
                                p.filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("5")),
                                        p.values(10, p.symbol("a"))),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1, p.symbol("a")),
                                p.filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("5")),
                                        p.values(10, p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                FULL,
                                p.filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("5")),
                                        p.values(10, p.symbol("a"))),
                                p.values(1, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testReplaceInnerJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x"))))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));
    }

    @Test
    public void testReplaceLeftJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                LEFT,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x"))))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));
    }

    @Test
    public void testReplaceRightJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                RIGHT,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x"))))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));
    }

    @Test
    public void testReplaceFullJoinWithProject()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                FULL,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x"))))),
                                p.values(5, p.symbol("c"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                FULL,
                                p.values(5, p.symbol("c")),
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));
    }

    @Test
    public void testRemoveOutputDuplicates()
    {
        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("a"), p.symbol("b")), ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("x"))))),
                                p.values(5, p.symbol("c")),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("a"), p.symbol("b"), p.symbol("a"), p.symbol("b")),
                                ImmutableList.of(p.symbol("c"), p.symbol("c")),
                                Optional.empty()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new LongLiteral("1")),
                                        "b", PlanMatchPattern.expression(new StringLiteral("x")),
                                        "c", PlanMatchPattern.expression(new SymbolReference("c"))),
                                values("c")));
    }

    @Test
    public void testNonDeterministicValues()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("rand")), ImmutableList.of(new Row(ImmutableList.of(randomFunction)))),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();

        FunctionCall uuidFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("uuid", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new ReplaceJoinOverConstantWithProject(tester().getMetadata()))
                .on(p ->
                        p.join(
                                INNER,
                                p.valuesOfExpressions(ImmutableList.of(p.symbol("uuid")), ImmutableList.of(new Row(ImmutableList.of(uuidFunction)))),
                                p.values(5, p.symbol("b"))))
                .doesNotFire();
    }
}
