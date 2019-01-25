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
package io.prestosql.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.matching.Match;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Memo;
import io.prestosql.sql.planner.iterative.PlanNodeMatcher;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Trait;
import io.prestosql.sql.planner.iterative.TraitSet;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.PlanWithTrait;
import io.prestosql.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.sql.planner.assertions.PlanAssert.assertPlan;
import static io.prestosql.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class RuleAssert
{
    private final Metadata metadata;
    private TestingStatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private Session session;
    private final Rule<?> rule;

    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    private TypeProvider types;
    private PlanNode plan;
    private Map<PlanNodeId, TraitSet> planNodeTraits;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    public RuleAssert(Metadata metadata, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, Rule rule, TransactionManager transactionManager, AccessControl accessControl)
    {
        this.metadata = metadata;
        this.statsCalculator = new TestingStatsCalculator(statsCalculator);
        this.costCalculator = costCalculator;
        this.session = session;
        this.rule = rule;
        this.transactionManager = transactionManager;
        this.accessControl = accessControl;
    }

    public RuleAssert setSystemProperty(String key, String value)
    {
        return withSession(Session.builder(session)
                .setSystemProperty(key, value)
                .build());
    }

    public RuleAssert withSession(Session session)
    {
        this.session = session;
        return this;
    }

    public RuleAssert overrideStats(String nodeId, PlanNodeStatsEstimate nodeStats)
    {
        statsCalculator.setNodeStats(new PlanNodeId(nodeId), nodeStats);
        return this;
    }

    public RuleAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkArgument(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        plan = planProvider.apply(builder);
        types = builder.getTypes();
        planNodeTraits = builder.getPlanNodeTraits();
        return this;
    }

    public void doesNotFire()
    {
        RuleApplication ruleApplication = applyRule();

        if (ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "Expected %s to not fire for:\n%s",
                    rule.getClass().getName(),
                    inTransaction(session -> textLogicalPlan(plan, ruleApplication.types, metadata.getFunctionRegistry(), StatsAndCosts.empty(), session, 2))));
        }
    }

    public void matches(PlanMatchPattern pattern)
    {
        RuleApplication ruleApplication = applyRule();
        TypeProvider types = ruleApplication.types;

        if (!ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "%s did not fire for:\n%s",
                    rule.getClass().getName(),
                    formatPlan(plan, types)));
        }

        PlanNode actual = ruleApplication.getTransformedPlan();

        if (actual == plan) { // plans are not comparable, so we can only ensure they are not the same instance
            fail(String.format(
                    "%s: rule fired but return the original plan:\n%s",
                    rule.getClass().getName(),
                    formatPlan(plan, types)));
        }

        if (!ImmutableSet.copyOf(plan.getOutputSymbols()).equals(ImmutableSet.copyOf(actual.getOutputSymbols()))) {
            fail(String.format(
                    "%s: output schema of transformed and original plans are not equivalent\n" +
                            "\texpected: %s\n" +
                            "\tactual:   %s",
                    rule.getClass().getName(),
                    plan.getOutputSymbols(),
                    actual.getOutputSymbols()));
        }

        inTransaction(session -> {
            assertPlan(session, metadata, ruleApplication.statsProvider, new Plan(actual, types, StatsAndCosts.empty()), ruleApplication.lookup, pattern);
            return null;
        });
    }

    private RuleApplication applyRule()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator(types.allTypes());
        Memo memo = new Memo(idAllocator, plan);
        range(0, memo.getGroupCount())
                .map(group -> group + 1)
                .forEach(group -> {
                    PlanNode node = memo.getNode(group);
                    if (node instanceof PlanWithTrait) {
                        memo.storeTrait(group, ((PlanWithTrait) node).getTrait());
                    }
                    planNodeTraits.getOrDefault(node.getId(), TraitSet.empty())
                            .getTraits()
                            .forEach(trait -> memo.storeTrait(group, trait));
                });
        Lookup lookup = memo.getLookup();
        int rootGroup = memo.getRootGroup();
        PlanNode memoRoot = memo.getNode(rootGroup);
        TraitSet traitSet = memo.getTraitSet(rootGroup);

        return inTransaction(session -> applyRule(rule, memoRoot, traitSet, ruleContext(statsCalculator, costCalculator, symbolAllocator, memo, lookup, session)));
    }

    private static <T> RuleApplication applyRule(Rule<T> rule, PlanNode planNode, TraitSet traitSet, Rule.Context context)
    {
        PlanNodeMatcher matcher = new PlanNodeMatcher(context.getLookup());
        Match<T> match = matcher.match(rule.getPattern(), planNode);

        Rule.Result result;
        if (!rule.isEnabled(context.getSession()) || match.isEmpty()) {
            result = Rule.Result.empty();
        }
        else {
            result = rule.apply(match.value(), match.captures(), traitSet, context);
        }

        return new RuleApplication(context.getLookup(), context.getStatsProvider(), context.getSymbolAllocator().getTypes(), result);
    }

    public void hasTrait(Trait trait)
    {
        RuleApplication ruleApplication = applyRule();

        if (!ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "%s did not fire for:\n%s",
                    rule.getClass().getName(),
                    formatPlan(plan, ruleApplication.types)));
        }

        assertEquals(trait, ruleApplication.getProducedTrait());
    }

    private String formatPlan(PlanNode plan, TypeProvider types)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, session, types);
        return inTransaction(session -> textLogicalPlan(plan, types, metadata.getFunctionRegistry(), StatsAndCosts.create(plan, statsProvider, costProvider), session, 2, false));
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }

    private Rule.Context ruleContext(StatsCalculator statsCalculator, CostCalculator costCalculator, SymbolAllocator symbolAllocator, Memo memo, Lookup lookup, Session session)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(memo), lookup, session, symbolAllocator.getTypes());
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(memo), session, symbolAllocator.getTypes());

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return idAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted() {}

            @Override
            public WarningCollector getWarningCollector()
            {
                return WarningCollector.NOOP;
            }
        };
    }

    private static class RuleApplication
    {
        private final Lookup lookup;
        private final StatsProvider statsProvider;
        private final TypeProvider types;
        private final Rule.Result result;

        public RuleApplication(Lookup lookup, StatsProvider statsProvider, TypeProvider types, Rule.Result result)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.types = requireNonNull(types, "types is null");
            this.result = requireNonNull(result, "result is null");
        }

        private boolean wasRuleApplied()
        {
            return !result.isEmpty();
        }

        public PlanNode getTransformedPlan()
        {
            return result.getTransformedPlan().orElseThrow(() -> new IllegalStateException("Rule did not produce transformed plan"));
        }

        public Trait getProducedTrait()
        {
            return result.getTrait().orElseThrow(() -> new IllegalStateException("Rule did not produce trait"));
        }
    }

    private static class TestingStatsCalculator
            implements StatsCalculator
    {
        private final StatsCalculator delegate;
        private final Map<PlanNodeId, PlanNodeStatsEstimate> stats = new HashMap<>();

        TestingStatsCalculator(StatsCalculator delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
        {
            if (stats.containsKey(node.getId())) {
                return stats.get(node.getId());
            }
            return delegate.calculateStats(node, sourceStats, lookup, session, types);
        }

        public void setNodeStats(PlanNodeId nodeId, PlanNodeStatsEstimate nodeStats)
        {
            stats.put(nodeId, nodeStats);
        }
    }
}
