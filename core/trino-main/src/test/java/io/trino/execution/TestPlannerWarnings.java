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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.events.DefaultEventCollector;
import io.trino.execution.events.EventCollector;
import io.trino.execution.events.EventCollectorConfig;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.TrinoEvent;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.fail;

public class TestPlannerWarnings
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testWarning()
    {
        List<TrinoWarning> warnings = createTestWarnings(3);
        List<WarningCode> warningCodes = warnings.stream()
                .map(TrinoWarning::getWarningCode)
                .collect(toImmutableList());
        assertPlannerWarnings(queryRunner, "SELECT * FROM NATION", ImmutableMap.of(), warningCodes, Optional.of(ImmutableList.of(new TestWarningsRule(warnings))));
    }

    @Test
    public void testEvent()
    {
        List<TrinoEvent> events = createTestEvents(5);
        List<String> eventMessages = events.stream()
                .map(TrinoEvent::getMessage)
                .collect(toImmutableList());
        assertPlannerEvents(queryRunner, "SELECT * FROM NATION", ImmutableMap.of(), eventMessages, Optional.of(ImmutableList.of(new TestEventsRule(events))));
    }

    public static void assertPlannerWarnings(LocalQueryRunner queryRunner, @Language("SQL") String sql, Map<String, String> sessionProperties, List<WarningCode> expectedWarnings, Optional<List<Rule<?>>> rules)
    {
        EventCollector eventCollector = planWithEventCollector(queryRunner, sql, sessionProperties, rules);
        Set<WarningCode> warnings = eventCollector.getWarnings().stream()
                .map(TrinoWarning::getWarningCode)
                .collect(toImmutableSet());
        for (WarningCode expectedWarning : expectedWarnings) {
            if (!warnings.contains(expectedWarning)) {
                fail("Expected warning: " + expectedWarning);
            }
        }
    }

    public static void assertPlannerEvents(LocalQueryRunner queryRunner, @Language("SQL") String sql, Map<String, String> sessionProperties, List<String> expectedEvents, Optional<List<Rule<?>>> rules)
    {
        EventCollector eventCollector = planWithEventCollector(queryRunner, sql, sessionProperties, rules);
        Set<String> events = eventCollector.getEvents().stream()
                .map(TrinoEvent::getMessage)
                .collect(toImmutableSet());
        for (String expectedEvent : expectedEvents) {
            if (!events.contains(expectedEvent)) {
                fail("Expected event: " + expectedEvent);
            }
        }
    }

    private static EventCollector planWithEventCollector(LocalQueryRunner queryRunner, @Language("SQL") String sql, Map<String, String> sessionProperties, Optional<List<Rule<?>>> rules)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get());
        sessionProperties.forEach(sessionBuilder::setSystemProperty);
        EventCollector eventCollector = new DefaultEventCollector(new EventCollectorConfig());
        try {
            queryRunner.inTransaction(sessionBuilder.build(), transactionSession -> {
                if (rules.isPresent()) {
                    createPlan(queryRunner, transactionSession, sql, eventCollector, rules.get());
                }
                else {
                    queryRunner.createPlan(transactionSession, sql, OPTIMIZED, false, eventCollector);
                }
                return null;
            });
        }
        catch (TrinoException e) {
            // ignore
        }
        return eventCollector;
    }

    private static Plan createPlan(LocalQueryRunner queryRunner, Session session, String sql, EventCollector eventCollector, List<Rule<?>> rules)
    {
        // Warnings from testing rules will be added
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.copyOf(rules));

        return queryRunner.createPlan(session, sql, ImmutableList.of(optimizer), OPTIMIZED, eventCollector);
    }

    public static List<TrinoWarning> createTestWarnings(int numberOfWarnings)
    {
        checkArgument(numberOfWarnings > 0, "numberOfWarnings must be > 0");
        ImmutableList.Builder<TrinoWarning> builder = ImmutableList.builder();
        range(1, numberOfWarnings)
                .mapToObj(code -> new TrinoWarning(new WarningCode(code, "testWarning"), "Test warning " + code))
                .forEach(builder::add);
        return builder.build();
    }

    private static List<TrinoEvent> createTestEvents(int numberOfEvents)
    {
        checkArgument(numberOfEvents > 0, "numberOfEvents must be > 0");
        ImmutableList.Builder<TrinoEvent> builder = ImmutableList.builder();
        range(1, numberOfEvents)
                .mapToObj(code -> new TrinoEvent("testEvent"))
                .forEach(builder::add);
        return builder.build();
    }

    public static class TestWarningsRule
            implements Rule<ProjectNode>
    {
        private final List<TrinoWarning> warnings;

        public TestWarningsRule(List<TrinoWarning> warnings)
        {
            this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            warnings.stream()
                    .forEach(context.getEventCollector()::add);
            return Result.empty();
        }
    }

    private static class TestEventsRule
            implements Rule<ProjectNode>
    {
        private final List<TrinoEvent> events;

        public TestEventsRule(List<TrinoEvent> events)
        {
            this.events = ImmutableList.copyOf(requireNonNull(events, "events is null"));
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            events.forEach(context.getEventCollector()::add);
            return Result.empty();
        }
    }
}
