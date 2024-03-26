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
public final class AssignUniqueId
        implements PlanNode
{
    private final PlanNodeId id;
    private final PlanNode source;
    private final Symbol idColumn;

    @JsonCreator
    public AssignUniqueId(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("idColumn") Symbol idColumn)
    {
        this.id = requireNonNull(id, "id is null");
        this.source = requireNonNull(source, "source is null");
        this.idColumn = requireNonNull(idColumn, "idColumn is null");
    }

    @Override
    public List<Symbol> outputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.outputSymbols())
                .add(idColumn)
                .build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
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

    @JsonProperty
    public Symbol getIdColumn()
    {
        return idColumn;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignUniqueId(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new AssignUniqueId(id(), Iterables.getOnlyElement(newChildren), idColumn);
    }
}
