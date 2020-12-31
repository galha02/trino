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
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.assertions.ExpectedValueProvider;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestSwapAdjacentWindowsBySpecifications
        extends BaseRuleTest
{
    private final MetadataManager metadata = createTestMetadataManager();
    private WindowNode.Frame frame;
    private ResolvedFunction resolvedFunction;

    public TestSwapAdjacentWindowsBySpecifications()
    {
        frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        resolvedFunction = metadata.resolveFunction(QualifiedName.of("avg"), fromTypes(BIGINT));
    }

    @Test
    public void doesNotFireOnPlanWithoutWindowFunctions()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.window(new WindowNode.Specification(
                                ImmutableList.of(p.symbol("a")),
                                Optional.empty()),
                        ImmutableMap.of(p.symbol("avg_1"),
                                new WindowNode.Function(resolvedFunction, ImmutableList.of(), frame, false)),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void subsetComesFirst()
    {
        String columnAAlias = "ALIAS_A";
        String columnBAlias = "ALIAS_B";

        ExpectedValueProvider<WindowNode.Specification> specificationA = specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());
        ExpectedValueProvider<WindowNode.Specification> specificationAB = specification(ImmutableList.of(columnAAlias, columnBAlias), ImmutableList.of(), ImmutableMap.of());

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.symbol("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1", DOUBLE),
                                        new WindowNode.Function(resolvedFunction, ImmutableList.of(new SymbolReference("a")), frame, false)),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2", DOUBLE),
                                                new WindowNode.Function(resolvedFunction, ImmutableList.of(new SymbolReference("b")), frame, false)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationAB)
                                        .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnBAlias))),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias))),
                                        values(ImmutableMap.of(columnAAlias, 0, columnBAlias, 1)))));
    }

    @Test
    public void dependentWindowsAreNotReordered()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.symbol("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.symbol("avg_1"),
                                        new WindowNode.Function(resolvedFunction, ImmutableList.of(new SymbolReference("avg_2")), frame, false)),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.symbol("avg_2"),
                                                new WindowNode.Function(resolvedFunction, ImmutableList.of(new SymbolReference("a")), frame, false)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }
}
