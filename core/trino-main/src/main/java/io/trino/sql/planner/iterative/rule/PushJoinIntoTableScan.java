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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.analyzer.FeaturesConfig.JoinPushdownMode;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getJoinPushdownMode;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushJoinIntoTableScan
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();

    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private final Metadata metadata;

    public PushJoinIntoTableScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.isCrossJoin()) {
            return Result.empty();
        }

        TableScanNode left = captures.get(LEFT_TABLE_SCAN);
        TableScanNode right = captures.get(RIGHT_TABLE_SCAN);

        verify(!left.isForDelete() && !right.isForDelete(), "Unexpected Join over for-delete table scan");

        Expression effectiveFilter = getEffectiveFilter(joinNode);
        FilterSplitResult filterSplitResult = splitFilter(effectiveFilter, joinNode.getLeftOutputSymbols(), joinNode.getRightOutputSymbols(), context);

        if (!filterSplitResult.getRemainingFilter().equals(BooleanLiteral.TRUE_LITERAL)) {
            // TODO add extra filter node above join
            return Result.empty();
        }

        if (left.getEnforcedConstraint().isNone() || right.getEnforcedConstraint().isNone()) {
            // bailing out on one of the tables empty; this is not interesting case which makes handling
            // enforced constraint harder below.
            return Result.empty();
        }

        Map<String, ColumnHandle> leftAssignments = left.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Map<String, ColumnHandle> rightAssignments = right.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Optional<JoinApplicationResult<TableHandle>> joinApplicationResult = metadata.applyJoin(
                context.getSession(),
                getJoinType(joinNode),
                left.getTable(),
                right.getTable(),
                filterSplitResult.getPushableConditions(),
                // TODO we could pass only subset of assignments here, those which are needed to resolve filterSplitResult.getPushableConditions
                leftAssignments,
                rightAssignments);

        if (joinApplicationResult.isEmpty()) {
            return Result.empty();
        }

        TableHandle handle = joinApplicationResult.get().getTableHandle();

        Map<ColumnHandle, ColumnHandle> leftColumnHandlesMapping = joinApplicationResult.get().getLeftColumnHandles();
        Map<ColumnHandle, ColumnHandle> rightColumnHandlesMapping = joinApplicationResult.get().getRightColumnHandles();

        ImmutableMap.Builder<Symbol, ColumnHandle> newAssignments = ImmutableMap.builder();
        newAssignments.putAll(left.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> leftColumnHandlesMapping.get(entry.getValue()))));
        newAssignments.putAll(right.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> rightColumnHandlesMapping.get(entry.getValue()))));

        // convert enforced constraint
        JoinNode.Type joinType = joinNode.getType();
        TupleDomain<ColumnHandle> leftConstraint = deriveConstraint(left.getEnforcedConstraint(), leftColumnHandlesMapping, joinType == RIGHT || joinType == FULL);
        TupleDomain<ColumnHandle> rightConstraint = deriveConstraint(right.getEnforcedConstraint(), rightColumnHandlesMapping, joinType == LEFT || joinType == FULL);

        TupleDomain<ColumnHandle> newEnforcedConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        // we are sure that domains map is present as we bailed out on isNone above
                        .putAll(leftConstraint.getDomains().orElseThrow())
                        .putAll(rightConstraint.getDomains().orElseThrow())
                        .build());

        return Result.ofPlanNode(new TableScanNode(joinNode.getId(), handle, joinNode.getOutputSymbols(), newAssignments.build(), newEnforcedConstraint, false));
    }

    private TupleDomain<ColumnHandle> deriveConstraint(TupleDomain<ColumnHandle> sourceConstraint, Map<ColumnHandle, ColumnHandle> columnMapping, boolean nullable)
    {
        TupleDomain<ColumnHandle> constraint = sourceConstraint;
        if (nullable) {
            constraint = constraint.transformDomains((columnHandle, domain) -> domain.union(onlyNull(domain.getType())));
        }
        return constraint.transform(handle -> {
            ColumnHandle newHandle = columnMapping.get(handle);
            checkArgument(newHandle != null, "Mapping not found for handle %s", handle);
            return newHandle;
        });
    }

    @Override
    public boolean isEnabled(Session session)
    {
        if (getJoinPushdownMode(session) == JoinPushdownMode.DISABLED) {
            return false;
        }
        if (!isAllowPushdownIntoConnectors(session)) {
            return false;
        }
        return true;
    }

    private TupleDomain<ColumnHandle> transformToNewAssignments(TupleDomain<ColumnHandle> tupleDomain, Map<ColumnHandle, ColumnHandle> newAssignments)
    {
        return tupleDomain.transform(handle -> {
            ColumnHandle newHandle = newAssignments.get(handle);
            checkArgument(newHandle != null, "Mapping not found for handle %s", handle);
            return newHandle;
        });
    }

    public Expression getEffectiveFilter(JoinNode node)
    {
        Expression effectiveFilter = and(node.getCriteria().stream().map(JoinNode.EquiJoinClause::toExpression).collect(toImmutableList()));
        if (node.getFilter().isPresent()) {
            effectiveFilter = and(effectiveFilter, node.getFilter().get());
        }
        return effectiveFilter;
    }

    private FilterSplitResult splitFilter(Expression filter, List<Symbol> leftSymbolsList, List<Symbol> rightSymbolsList, Context context)
    {
        Set<Symbol> leftSymbols = ImmutableSet.copyOf(leftSymbolsList);
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(rightSymbolsList);

        ImmutableList.Builder<JoinCondition> comparisonConditions = ImmutableList.builder();
        ImmutableList.Builder<Expression> remainingConjuncts = ImmutableList.builder();

        for (Expression conjunct : extractConjuncts(filter)) {
            getPushableJoinCondition(conjunct, leftSymbols, rightSymbols, context)
                    .ifPresentOrElse(comparisonConditions::add, () -> remainingConjuncts.add(conjunct));
        }

        return new FilterSplitResult(comparisonConditions.build(), ExpressionUtils.and(remainingConjuncts.build()));
    }

    private Optional<JoinCondition> getPushableJoinCondition(Expression conjunct, Set<Symbol> leftSymbols, Set<Symbol> rightSymbols, Context context)
    {
        if (!(conjunct instanceof ComparisonExpression)) {
            return Optional.empty();
        }
        ComparisonExpression comparison = (ComparisonExpression) conjunct;

        if (!(comparison.getLeft() instanceof SymbolReference) || !(comparison.getRight() instanceof SymbolReference)) {
            return Optional.empty();
        }
        Symbol left = Symbol.from(comparison.getLeft());
        Symbol right = Symbol.from(comparison.getRight());
        ComparisonExpression.Operator operator = comparison.getOperator();

        if (!leftSymbols.contains(left)) {
            // lets try with flipped expression
            Symbol tmp = left;
            left = right;
            right = tmp;
            operator = operator.flip();
        }

        if (leftSymbols.contains(left) && rightSymbols.contains(right)) {
            return Optional.of(new JoinCondition(
                    joinConditionOperator(operator),
                        new Variable(left.getName(), context.getSymbolAllocator().getTypes().get(left)),
                        new Variable(right.getName(), context.getSymbolAllocator().getTypes().get(right))));
        }
        return Optional.empty();
    }

    private static class FilterSplitResult
    {
        private final List<JoinCondition> pushableConditions;
        private final Expression remainingFilter;

        public FilterSplitResult(List<JoinCondition> pushableConditions, Expression remainingFilter)
        {
            this.pushableConditions = requireNonNull(pushableConditions, "pushableConditions is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        }

        public List<JoinCondition> getPushableConditions()
        {
            return pushableConditions;
        }

        public Expression getRemainingFilter()
        {
            return remainingFilter;
        }
    }

    private JoinCondition.Operator joinConditionOperator(ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return JoinCondition.Operator.EQUAL;
            case NOT_EQUAL:
                return JoinCondition.Operator.NOT_EQUAL;
            case LESS_THAN:
                return JoinCondition.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return JoinCondition.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return JoinCondition.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return JoinCondition.Operator.GREATER_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return JoinCondition.Operator.IS_DISTINCT_FROM;
        }
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }

    private JoinType getJoinType(JoinNode joinNode)
    {
        switch (joinNode.getType()) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT_OUTER;
            case RIGHT:
                return JoinType.RIGHT_OUTER;
            case FULL:
                return JoinType.FULL_OUTER;
        }
        throw new IllegalArgumentException("Unknown join type: " + joinNode.getType());
    }
}
