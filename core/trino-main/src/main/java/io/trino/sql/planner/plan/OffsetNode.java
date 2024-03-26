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
import io.trino.sql.planner.Symbol;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class OffsetNode
        implements PlanNode
{
    private final PlanNodeId id;
    private final PlanNode source;
    private final long count;

    @JsonCreator
    public OffsetNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count)
    {
        requireNonNull(id, "id is null");
        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be greater than or equal to zero");

        this.id = id;
        this.source = source;
        this.count = count;
    }

    @Override
    public PlanNodeId id()
    {
        return id;
    }

    @Override
    public List<PlanNode> sources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @Override
    public List<Symbol> outputSymbols()
    {
        return source.outputSymbols();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitOffset(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new OffsetNode(id(), Iterables.getOnlyElement(newChildren), count);
    }
}
