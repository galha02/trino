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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.metadata.AbstractMockMetadata.dummyMetadata;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestLocalDynamicFilterConsumer
        extends BasePlanTest
{
    public TestLocalDynamicFilterConsumer()
    {
        super(ImmutableMap.of(
                FORCE_SINGLE_NODE_OUTPUT, "false",
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testSimple()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L))));
        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 7L)));
    }

    @Test
    public void testShortCircuitOnAllTupleDomain()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                2);

        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.all(INTEGER))));
        assertEquals(result.get(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));

        // adding another partition domain won't change final domain
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 1L))));
        assertEquals(result.get(), ImmutableMap.of(new DynamicFilterId("123"), Domain.all(INTEGER)));
    }

    @Test
    public void testMultiplePartitions()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L))));

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L))));
    }

    @Test
    public void testAllDomain()
            throws Exception
    {
        DynamicFilterId filter1 = new DynamicFilterId("123");
        DynamicFilterId filter2 = new DynamicFilterId("124");
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(
                        filter1, 0,
                        filter2, 1),
                ImmutableMap.of(
                        filter1, INTEGER,
                        filter2, INTEGER),
                1);

        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                filter1, Domain.all(INTEGER),
                filter2, Domain.singleValue(INTEGER, 1L))));
        assertEquals(result.get(), ImmutableMap.of(filter1, Domain.all(INTEGER), filter2, Domain.singleValue(INTEGER, 1L)));
    }

    @Test
    public void testNone()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.none());

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.none(INTEGER)));
    }

    @Test
    public void testMultipleColumns()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), INTEGER),
                1);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();
        assertFalse(result.isDone());

        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L))));
        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(INTEGER, 20L)));
    }

    @Test
    public void testMultiplePartitionsAndColumns()
            throws Exception
    {
        LocalDynamicFilterConsumer filter = new LocalDynamicFilterConsumer(
                ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1),
                ImmutableMap.of(new DynamicFilterId("123"), INTEGER, new DynamicFilterId("456"), BIGINT),
                2);
        assertEquals(filter.getBuildChannels(), ImmutableMap.of(new DynamicFilterId("123"), 0, new DynamicFilterId("456"), 1));
        Consumer<TupleDomain<DynamicFilterId>> consumer = filter.getTupleDomainConsumer();
        ListenableFuture<Map<DynamicFilterId, Domain>> result = filter.getDynamicFilterDomains();

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 10L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 100L))));

        assertFalse(result.isDone());
        consumer.accept(TupleDomain.withColumnDomains(ImmutableMap.of(
                new DynamicFilterId("123"), Domain.singleValue(INTEGER, 20L),
                new DynamicFilterId("456"), Domain.singleValue(BIGINT, 200L))));

        assertEquals(result.get(), ImmutableMap.of(
                new DynamicFilterId("123"), Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)),
                new DynamicFilterId("456"), Domain.multipleValues(BIGINT, ImmutableList.of(100L, 200L))));
    }

    @Test
    public void testDynamicFilterPruning()
            throws Exception
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        Symbol left1 = planBuilder.symbol("left1", BIGINT);
        Symbol left2 = planBuilder.symbol("left2", INTEGER);
        Symbol left3 = planBuilder.symbol("left3", SMALLINT);
        Symbol right1 = planBuilder.symbol("right1", BIGINT);
        Symbol right2 = planBuilder.symbol("right2", INTEGER);
        Symbol right3 = planBuilder.symbol("right3", SMALLINT);
        DynamicFilterId filter1 = new DynamicFilterId("filter1");
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        DynamicFilterId filter3 = new DynamicFilterId("filter3");
        JoinNode joinNode = planBuilder.join(
                INNER,
                planBuilder.values(left1, left2, left3),
                planBuilder.values(right1, right2, right3),
                ImmutableList.of(
                        new EquiJoinClause(left1, right1),
                        new EquiJoinClause(left2, right2),
                        new EquiJoinClause(left3, right3)),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(filter1, right1, filter2, right2, filter3, right3));
        LocalDynamicFilterConsumer consumer = LocalDynamicFilterConsumer.create(
                joinNode,
                ImmutableList.of(BIGINT, INTEGER, SMALLINT),
                1,
                ImmutableSet.of(filter1, filter3));
        assertEquals(consumer.getBuildChannels(), ImmutableMap.of(filter1, 0, filter3, 2));

        // make sure domain types got propagated correctly
        consumer.getTupleDomainConsumer().accept(TupleDomain.none());
        assertEquals(
                consumer.getDynamicFilterDomains().get(),
                ImmutableMap.of(filter1, Domain.none(BIGINT), filter3, Domain.none(SMALLINT)));
    }
}
