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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SetOperationNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class SetOperationOutputMatcher
        implements RvalueMatcher
{
    private final int index;

    public SetOperationOutputMatcher(int index)
    {
        checkArgument(index >= 0, "index cannot be negative");
        this.index = index;
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof SetOperationNode)) {
            return Optional.empty();
        }

        SetOperationNode setOperationNode = (SetOperationNode) node;

        if (index >= setOperationNode.getOutputSymbols().size()) {
            return Optional.empty();
        }

        return Optional.of(node.getOutputSymbols().get(index));
    }
}
