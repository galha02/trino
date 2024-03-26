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
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public final class FilterNode
        implements PlanNode
{
    private final PlanNodeId id;
    private final PlanNode source;
    private final Expression predicate;

    @JsonCreator
    public FilterNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("predicate") Expression predicate)
    {
        this.id = requireNonNull(id, "id is null");
        this.source = source;
        requireNonNull(predicate, "predicate is null");
        this.predicate = predicate;
    }

    @JsonProperty("predicate")
    public Expression getPredicate()
    {
        return predicate;
    }

    @Override
    public List<Symbol> outputSymbols()
    {
        return source.outputSymbols();
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
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new FilterNode(id(), Iterables.getOnlyElement(newChildren), predicate);
    }
}
