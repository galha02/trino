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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Variable;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.util.MoreLists.filteredCopy;
import static java.util.Objects.requireNonNull;

/**
 * TODO: this is a special case of PushProjectionIntoTableScan and should be merged with that rule.
 */
public class PruneTableScanColumns
        extends ProjectOffPushDownRule<TableScanNode>
{
    private final Metadata metadata;

    public PruneTableScanColumns(Metadata metadata)
    {
        super(tableScan());
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, TableScanNode node, Set<Symbol> referencedOutputs)
    {
        Session session = context.getSession();
        TypeProvider types = context.getSymbolAllocator().getTypes();

        return pruneColumns(metadata, types, session, node, referencedOutputs);
    }

    public static Optional<PlanNode> pruneColumns(Metadata metadata, TypeProvider types, Session session, TableScanNode node, Set<Symbol> referencedOutputs)
    {
        List<Symbol> newOutputs = filteredCopy(node.getOutputSymbols(), referencedOutputs::contains);

        if (newOutputs.size() == node.getOutputSymbols().size()) {
            return Optional.empty();
        }

        List<ConnectorExpression> projections = newOutputs.stream()
                .map(symbol -> new Variable(symbol.getName(), types.get(symbol)))
                .collect(toImmutableList());

        TableHandle handle = node.getTable();
        Optional<ProjectionApplicationResult<TableHandle>> result = metadata.applyProjection(
                session,
                handle,
                projections,
                newOutputs.stream()
                        .collect(toImmutableMap(Symbol::getName, node.getAssignments()::get)));

        Map<Symbol, ColumnHandle> newAssignments;
        // Attempt to push down the constrained list of columns into the connector.
        // Bail out if the connector does anything other than limit the list of columns (e.g., if it synthesizes arbitrary expressions)
        if (result.isPresent() && result.get().getProjections().stream().allMatch(Variable.class::isInstance)) {
            handle = result.get().getHandle();

            Map<String, ColumnHandle> assignments = result.get().getAssignments().stream()
                    .collect(toImmutableMap(ProjectionApplicationResult.Assignment::getVariable, ProjectionApplicationResult.Assignment::getColumn));

            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (int i = 0; i < newOutputs.size(); i++) {
                Variable variable = (Variable) result.get().getProjections().get(i);
                builder.put(newOutputs.get(i), assignments.get(variable.getName()));
            }

            newAssignments = builder.build();
        }
        else {
            newAssignments = newOutputs.stream()
                    .collect(toImmutableMap(Function.identity(), node.getAssignments()::get));
        }

        return Optional.of(new TableScanNode(
                node.getId(),
                handle,
                newOutputs,
                newAssignments,
                node.getEnforcedConstraint()));
    }
}
