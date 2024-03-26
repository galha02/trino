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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNNode
        implements PlanNode
{
    private final PlanNodeId id;

    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }

    private final PlanNode source;
    private final long count;
    private final OrderingScheme orderingScheme;
    private final Step step;

    @JsonCreator
    public TopNNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("step") Step step)
    {
        requireNonNull(id, "id is null");
        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be positive");
        checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "ORDER BY LIMIT > %s is not supported", Integer.MAX_VALUE);
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.id = id;
        this.source = source;
        this.count = count;
        this.orderingScheme = orderingScheme;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    @JsonProperty
    public PlanNodeId id()
    {
        return id;
    }

    @Override
    public List<PlanNode> sources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> outputSymbols()
    {
        return source.outputSymbols();
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("orderingScheme")
    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TopNNode(id(), Iterables.getOnlyElement(newChildren), count, orderingScheme, step);
    }
}
