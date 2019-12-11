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
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.Objects.requireNonNull;

final class SemiJoinMatcher
        implements Matcher
{
    private final String sourceSymbolAlias;
    private final String filteringSymbolAlias;
    private final String outputAlias;
    private final Optional<SemiJoinNode.DistributionType> distributionType;
    private final boolean dynamicFilteringEnabled;
    private Optional<Expression> expectedStaticFilterCollocatingWithDynamicFilter;

    SemiJoinMatcher(
            String sourceSymbolAlias,
            String filteringSymbolAlias,
            String outputAlias,
            Optional<SemiJoinNode.DistributionType> distributionType,
            boolean dynamicFilteringEnabled,
            Optional<Expression> expectedStaticFilterCollocatingWithDynamicFilter)
    {
        this.sourceSymbolAlias = requireNonNull(sourceSymbolAlias, "sourceSymbolAlias is null");
        this.filteringSymbolAlias = requireNonNull(filteringSymbolAlias, "filteringSymbolAlias is null");
        this.outputAlias = requireNonNull(outputAlias, "outputAlias is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.dynamicFilteringEnabled = dynamicFilteringEnabled;
        this.expectedStaticFilterCollocatingWithDynamicFilter = requireNonNull(expectedStaticFilterCollocatingWithDynamicFilter, "expectedStaticFilterCollocatingWithDynamicFilter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof SemiJoinNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        SemiJoinNode semiJoinNode = (SemiJoinNode) node;
        if (!(symbolAliases.get(sourceSymbolAlias).equals(semiJoinNode.getSourceJoinSymbol().toSymbolReference()) &&
                symbolAliases.get(filteringSymbolAlias).equals(semiJoinNode.getFilteringSourceJoinSymbol().toSymbolReference()))) {
            return NO_MATCH;
        }

        if (distributionType.isPresent() && !distributionType.equals(semiJoinNode.getDistributionType())) {
            return NO_MATCH;
        }

        if (dynamicFilteringEnabled) {
            if (!semiJoinNode.getDynamicFilterId().isPresent()) {
                return NO_MATCH;
            }
            String dynamicFilterId = semiJoinNode.getDynamicFilterId().get();
            List<FilterNode> filters = searchFrom(semiJoinNode.getSource())
                    .where(FilterNode.class::isInstance)
                    .findAll();
            for (FilterNode filter : filters) {
                DynamicFilters.ExtractResult extractResult = extractDynamicFilters(metadata, filter.getPredicate());
                Optional<DynamicFilters.Descriptor> dynamicFilter = extractResult.getDynamicConjuncts().stream()
                        .filter(descriptor -> descriptor.getId().equals(dynamicFilterId))
                        .findAny();
                if (dynamicFilter.isPresent()) {
                    boolean staticFilterMatches = expectedStaticFilterCollocatingWithDynamicFilter.map(staticfilter -> {
                        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
                        Expression staticFilter = combineConjuncts(metadata, extractResult.getStaticConjuncts());
                        return verifier.process(staticFilter, staticfilter);
                    }).orElse(true);
                    if (!staticFilterMatches) {
                        return NO_MATCH;
                    }
                    Symbol dynamicFilterSymbol = Symbol.from(dynamicFilter.get().getInput());
                    if (dynamicFilterSymbol.equals(semiJoinNode.getFilteringSourceJoinSymbol())) {
                        return NO_MATCH;
                    }
                    return match(outputAlias, semiJoinNode.getSemiJoinOutput().toSymbolReference());
                }
            }
            return NO_MATCH;
        }

        return match(outputAlias, semiJoinNode.getSemiJoinOutput().toSymbolReference());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filteringSymbolAlias", filteringSymbolAlias)
                .add("sourceSymbolAlias", sourceSymbolAlias)
                .add("outputAlias", outputAlias)
                .add("distributionType", distributionType)
                .toString();
    }
}
