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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.FunctionId;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isOptimizeTopNRanking;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Marker.Bound.BELOW;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.DomainTranslator.fromPredicate;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.window;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.Math.toIntExact;

public class PushdownFilterIntoWindow
        implements Rule<FilterNode>
{
    private static final Capture<WindowNode> childCapture = newCapture();

    private final Pattern<FilterNode> pattern;
    private final Metadata metadata;
    private final DomainTranslator domainTranslator;
    private final FunctionId rowNumberFunctionId;
    private final FunctionId rankFunctionId;
    private final TypeOperators typeOperators;

    public PushdownFilterIntoWindow(Metadata metadata, TypeOperators typeOperators)
    {
        this.metadata = metadata;
        this.domainTranslator = new DomainTranslator(metadata);
        this.rowNumberFunctionId = metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId();
        this.rankFunctionId = metadata.resolveFunction(QualifiedName.of("rank"), ImmutableList.of()).getFunctionId();
        this.pattern = filter()
                .with(source().matching(window()
                        .matching(window -> window.getOrderingScheme().isPresent())
                        .matching(window -> toTopNRankingType(window).isPresent())
                        .capturedAs(childCapture)));
        this.typeOperators = typeOperators;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return pattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeTopNRanking(session);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        TypeProvider types = context.getSymbolAllocator().getTypes();

        WindowNode windowNode = captures.get(childCapture);

        DomainTranslator.ExtractionResult extractionResult = fromPredicate(metadata, typeOperators, session, node.getPredicate(), types);
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

        Optional<RankingType> rankingType = toTopNRankingType(windowNode);

        Symbol rankingSymbol = getOnlyElement(windowNode.getWindowFunctions().keySet());
        OptionalInt upperBound = extractUpperBound(tupleDomain, rankingSymbol);

        if (upperBound.isEmpty()) {
            return Result.empty();
        }

        if (upperBound.getAsInt() <= 0) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }
        TopNRankingNode newSource = new TopNRankingNode(
                windowNode.getId(),
                windowNode.getSource(),
                windowNode.getSpecification(),
                rankingType.get(),
                rankingSymbol,
                upperBound.getAsInt(),
                false,
                Optional.empty());

        if (!allRowNumberValuesInDomain(tupleDomain, rankingSymbol, upperBound.getAsInt())) {
            return Result.ofPlanNode(new FilterNode(node.getId(), newSource, node.getPredicate()));
        }

        // Remove the row number domain because it is absorbed into the node
        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rankingSymbol));
        Expression newPredicate = ExpressionUtils.combineConjuncts(
                metadata,
                extractionResult.getRemainingExpression(),
                domainTranslator.toPredicate(newTupleDomain));

        if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
            return Result.ofPlanNode(newSource);
        }
        return Result.ofPlanNode(new FilterNode(node.getId(), newSource, newPredicate));
    }

    private static boolean allRowNumberValuesInDomain(TupleDomain<Symbol> tupleDomain, Symbol symbol, long upperBound)
    {
        if (tupleDomain.isNone()) {
            return false;
        }
        Domain domain = tupleDomain.getDomains().get().get(symbol);
        if (domain == null) {
            return true;
        }
        return domain.getValues().contains(ValueSet.ofRanges(range(domain.getType(), 1L, true, upperBound, true)));
    }

    private static OptionalInt extractUpperBound(TupleDomain<Symbol> tupleDomain, Symbol symbol)
    {
        if (tupleDomain.isNone()) {
            return OptionalInt.empty();
        }

        Domain rowNumberDomain = tupleDomain.getDomains().get().get(symbol);
        if (rowNumberDomain == null) {
            return OptionalInt.empty();
        }
        ValueSet values = rowNumberDomain.getValues();
        if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
            return OptionalInt.empty();
        }

        Range span = values.getRanges().getSpan();

        if (span.getHigh().isUpperUnbounded()) {
            return OptionalInt.empty();
        }

        verify(rowNumberDomain.getType().equals(BIGINT));
        long upperBound = (Long) span.getHigh().getValue();
        if (span.getHigh().getBound() == BELOW) {
            upperBound--;
        }

        if (upperBound >= Integer.MIN_VALUE && upperBound <= Integer.MAX_VALUE) {
            return OptionalInt.of(toIntExact(upperBound));
        }
        return OptionalInt.empty();
    }

    private Optional<RankingType> toTopNRankingType(WindowNode node)
    {
        if (node.getWindowFunctions().size() != 1 || node.getOrderingScheme().isEmpty()) {
            return Optional.empty();
        }
        Symbol rankingSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
        FunctionId functionId = node.getWindowFunctions().get(rankingSymbol).getResolvedFunction().getFunctionId();
        if (functionId.equals(rowNumberFunctionId)) {
            return Optional.of(ROW_NUMBER);
        }
        if (functionId.equals(rankFunctionId)) {
            return Optional.of(RANK);
        }
        return Optional.empty();
    }
}
