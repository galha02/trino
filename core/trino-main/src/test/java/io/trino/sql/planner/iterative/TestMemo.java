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

import com.google.common.collect.ImmutableList;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.GenericNode;
import io.trino.sql.planner.plan.GroupReference;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMemo
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testInitialization()
    {
        PlanNode plan = node(node());
        Memo memo = new Memo(idAllocator, plan);

        assertThat(memo.getGroupCount()).isEqualTo(2);
        assertMatchesStructure(plan, memo.extract());
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z'
     */
    @Test
    public void testReplaceSubtree()
    {
        PlanNode plan = node(node(node()));

        Memo memo = new Memo(idAllocator, plan);
        assertThat(memo.getGroupCount()).isEqualTo(3);

        // replace child of root node with subtree
        PlanNode transformed = node(node());
        memo.replace(getChildGroup(memo, memo.getRootGroup()), transformed, "rule");
        assertThat(memo.getGroupCount()).isEqualTo(3);
        assertMatchesStructure(memo.extract(), node(plan.id(), transformed));
    }

    /*
      From: X -> Y  -> Z
      To:   X -> Y' -> Z
     */
    @Test
    public void testReplaceNode()
    {
        PlanNode z = node();
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);
        assertThat(memo.getGroupCount()).isEqualTo(3);

        // replace child of root node with another node, retaining child's child
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        GroupReference zRef = (GroupReference) getOnlyElement(memo.getNode(yGroup).sources());
        PlanNode transformed = node(zRef);
        memo.replace(yGroup, transformed, "rule");
        assertThat(memo.getGroupCount()).isEqualTo(3);
        assertMatchesStructure(memo.extract(), node(x.id(), node(transformed.id(), z)));
    }

    /*
      From: X -> Y  -> Z  -> W
      To:   X -> Y' -> Z' -> W
     */
    @Test
    public void testReplaceNonLeafSubtree()
    {
        PlanNode w = node();
        PlanNode z = node(w);
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertThat(memo.getGroupCount()).isEqualTo(4);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        int zGroup = getChildGroup(memo, yGroup);

        PlanNode rewrittenW = memo.getNode(zGroup).sources().get(0);

        PlanNode newZ = node(rewrittenW);
        PlanNode newY = node(newZ);

        memo.replace(yGroup, newY, "rule");

        assertThat(memo.getGroupCount()).isEqualTo(4);

        assertMatchesStructure(
                memo.extract(),
                node(x.id(),
                        node(newY.id(),
                                node(newZ.id(),
                                        node(w.id())))));
    }

    /*
      From: X -> Y -> Z
      To:   X -> Z
     */
    @Test
    public void testRemoveNode()
    {
        PlanNode z = node();
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);

        assertThat(memo.getGroupCount()).isEqualTo(3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());
        memo.replace(yGroup, memo.getNode(yGroup).sources().get(0), "rule");

        assertThat(memo.getGroupCount()).isEqualTo(2);

        assertMatchesStructure(
                memo.extract(),
                node(x.id(),
                        node(z.id())));
    }

    /*
       From: X -> Z
       To:   X -> Y -> Z
     */
    @Test
    public void testInsertNode()
    {
        PlanNode z = node();
        PlanNode x = node(z);

        Memo memo = new Memo(idAllocator, x);

        assertThat(memo.getGroupCount()).isEqualTo(2);

        int zGroup = getChildGroup(memo, memo.getRootGroup());
        PlanNode y = node(memo.getNode(zGroup));
        memo.replace(zGroup, y, "rule");

        assertThat(memo.getGroupCount()).isEqualTo(3);

        assertMatchesStructure(
                memo.extract(),
                node(x.id(),
                        node(y.id(),
                                node(z.id()))));
    }

    /*
      From: X -> Y -> Z
      To:   X --> Y1' --> Z
              \-> Y2' -/
     */
    @Test
    public void testMultipleReferences()
    {
        PlanNode z = node();
        PlanNode y = node(z);
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);
        assertThat(memo.getGroupCount()).isEqualTo(3);

        int yGroup = getChildGroup(memo, memo.getRootGroup());

        PlanNode rewrittenZ = memo.getNode(yGroup).sources().get(0);
        PlanNode y1 = node(rewrittenZ);
        PlanNode y2 = node(rewrittenZ);

        PlanNode newX = node(y1, y2);
        memo.replace(memo.getRootGroup(), newX, "rule");
        assertThat(memo.getGroupCount()).isEqualTo(4);

        assertMatchesStructure(
                memo.extract(),
                node(newX.id(),
                        node(y1.id(), node(z.id())),
                        node(y2.id(), node(z.id()))));
    }

    @Test
    public void testEvictStatsOnReplace()
    {
        PlanNode y = node();
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);
        int xGroup = memo.getRootGroup();
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        PlanNodeStatsEstimate xStats = PlanNodeStatsEstimate.builder().setOutputRowCount(42).build();
        PlanNodeStatsEstimate yStats = PlanNodeStatsEstimate.builder().setOutputRowCount(55).build();

        memo.storeStats(yGroup, yStats);
        memo.storeStats(xGroup, xStats);

        assertThat(memo.getStats(yGroup)).isEqualTo(Optional.of(yStats));
        assertThat(memo.getStats(xGroup)).isEqualTo(Optional.of(xStats));

        memo.replace(yGroup, node(), "rule");

        assertThat(memo.getStats(yGroup)).isEqualTo(Optional.empty());
        assertThat(memo.getStats(xGroup)).isEqualTo(Optional.empty());
    }

    @Test
    public void testEvictCostOnReplace()
    {
        PlanNode y = node();
        PlanNode x = node(y);

        Memo memo = new Memo(idAllocator, x);
        int xGroup = memo.getRootGroup();
        int yGroup = getChildGroup(memo, memo.getRootGroup());
        PlanCostEstimate yCost = new PlanCostEstimate(42, 0, 0, 0);
        PlanCostEstimate xCost = new PlanCostEstimate(42, 0, 0, 37);

        memo.storeCost(yGroup, yCost);
        memo.storeCost(xGroup, xCost);

        assertThat(memo.getCost(yGroup)).isEqualTo(Optional.of(yCost));
        assertThat(memo.getCost(xGroup)).isEqualTo(Optional.of(xCost));

        memo.replace(yGroup, node(), "rule");

        assertThat(memo.getCost(yGroup)).isEqualTo(Optional.empty());
        assertThat(memo.getCost(xGroup)).isEqualTo(Optional.empty());
    }

    private static void assertMatchesStructure(PlanNode actual, PlanNode expected)
    {
        assertThat(actual.getClass()).isEqualTo(expected.getClass());
        assertThat(actual.id()).isEqualTo(expected.id());
        assertThat(actual.sources().size()).isEqualTo(expected.sources().size());

        for (int i = 0; i < actual.sources().size(); i++) {
            assertMatchesStructure(actual.sources().get(i), expected.sources().get(i));
        }
    }

    private int getChildGroup(Memo memo, int group)
    {
        PlanNode node = memo.getNode(group);
        GroupReference child = (GroupReference) node.sources().get(0);

        return child.getGroupId();
    }

    private GenericNode node(PlanNodeId id, PlanNode... children)
    {
        return new GenericNode(id, ImmutableList.copyOf(children));
    }

    private GenericNode node(PlanNode... children)
    {
        return node(idAllocator.getNextId(), children);
    }
}
