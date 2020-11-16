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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExceptNode;
import io.prestosql.sql.planner.plan.IntersectNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;

public class PruneDistinctAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PruneDistinctAggregation::isDistinctOperator);

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        DistinctAggregationRewriter rewriter = new DistinctAggregationRewriter(lookup);

        List<PlanNode> newSources = node.getSources().stream()
                .flatMap(lookup::resolveGroup)
                .map(source -> source.accept(rewriter, true))
                .collect(Collectors.toList());

        if (rewriter.isRewritten()) {
            return Result.ofPlanNode(replaceChildren(node, newSources));
        }
        else {
            return Result.empty();
        }
    }

    private static boolean isDistinctOperator(AggregationNode node)
    {
        return node.getAggregations().isEmpty();
    }

    private static class DistinctAggregationRewriter
            extends PlanVisitor<PlanNode, Boolean>
    {
        private final Lookup lookup;
        private boolean rewritten;

        public DistinctAggregationRewriter(Lookup lookup)
        {
            this.lookup = lookup;
            this.rewritten = false;
        }

        public boolean isRewritten()
        {
            return rewritten;
        }

        private PlanNode rewriteChildren(PlanNode node, Boolean context)
        {
            List<PlanNode> newSources = node.getSources().stream()
                    .flatMap(lookup::resolveGroup)
                    .map(source -> source.accept(this, context)).collect(Collectors.toList());

            return replaceChildren(node, newSources);
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Boolean context)
        {
            // Unable to remove distinct aggregation anymore.
            return rewriteChildren(node, false);
        }

        @Override
        public PlanNode visitUnion(UnionNode node, Boolean context)
        {
            return rewriteChildren(node, context);
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, Boolean context)
        {
            if (node.isDistinct()) {
                return rewriteChildren(node, context);
            }
            return visitPlan(node, context);
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, Boolean context)
        {
            if (node.isDistinct()) {
                return rewriteChildren(node, context);
            }
            return visitPlan(node, context);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Boolean context)
        {
            boolean distinct = isDistinctOperator(node);

            PlanNode rewrittenNode = getOnlyElement(lookup.resolveGroup(node.getSource())
                    .map(source -> source.accept(this, distinct)).collect(Collectors.toList()));

            if (context && distinct) {
                this.rewritten = true;
                // Assumes underlying node has same output symbols as this distinct node
                return rewrittenNode;
            }

            return new AggregationNode(
                    node.getId(),
                    rewrittenNode,
                    node.getAggregations(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }
    }
}
