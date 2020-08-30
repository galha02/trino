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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ListMultimap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.BoundSignature;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.type.FunctionType;
import io.prestosql.type.TypeCoercion;
import io.prestosql.type.UnknownType;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Ensures that all the expressions and FunctionCalls matches their output symbols
 */
public final class TypeValidator
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        plan.accept(new Visitor(session, metadata, typeAnalyzer, types, warningCollector), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final Session session;
        private final TypeCoercion typeCoercion;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;
        private final WarningCollector warningCollector;

        public Visitor(Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.typeCoercion = new TypeCoercion(metadata::getType);
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);

            AggregationNode.Step step = node.getStep();

            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                switch (step) {
                    case SINGLE:
                        checkSignature(symbol, aggregation.getResolvedFunction().getSignature());
                        checkCall(symbol, aggregation.getResolvedFunction().getSignature(), aggregation.getArguments());
                        break;
                    case FINAL:
                        checkSignature(symbol, aggregation.getResolvedFunction().getSignature());
                        break;
                }
            }

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);

            checkWindowFunctions(node.getWindowFunctions());

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);

            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Type expectedType = types.get(entry.getKey());
                if (entry.getValue() instanceof SymbolReference) {
                    SymbolReference symbolReference = (SymbolReference) entry.getValue();
                    verifyTypeSignature(entry.getKey(), expectedType, types.get(Symbol.from(symbolReference)));
                    continue;
                }
                Type actualType = typeAnalyzer.getType(session, types, entry.getValue());
                verifyTypeSignature(entry.getKey(), expectedType, actualType);
            }

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            visitPlan(node, context);

            ListMultimap<Symbol, Symbol> symbolMapping = node.getSymbolMapping();
            for (Symbol keySymbol : symbolMapping.keySet()) {
                List<Symbol> valueSymbols = symbolMapping.get(keySymbol);
                Type expectedType = types.get(keySymbol);
                for (Symbol valueSymbol : valueSymbols) {
                    verifyTypeSignature(keySymbol, expectedType, types.get(valueSymbol));
                }
            }

            return null;
        }

        private void checkWindowFunctions(Map<Symbol, WindowNode.Function> functions)
        {
            functions.forEach((symbol, function) -> {
                checkSignature(symbol, function.getResolvedFunction().getSignature());
                checkCall(symbol, function.getResolvedFunction().getSignature(), function.getArguments());
            });
        }

        private void checkSignature(Symbol symbol, BoundSignature signature)
        {
            Type expectedType = types.get(symbol);
            Type actualType = signature.getReturnType();
            verifyTypeSignature(symbol, expectedType, actualType);
        }

        private void checkCall(Symbol symbol, BoundSignature signature, List<Expression> arguments)
        {
            Type expectedType = types.get(symbol);
            Type actualType = signature.getReturnType();
            verifyTypeSignature(symbol, expectedType, actualType);

            checkArgument(signature.getArgumentTypes().size() == arguments.size(),
                    "expected %s arguments, but found %s arguments",
                    signature.getArgumentTypes().size(),
                    arguments.size());

            for (int i = 0; i < arguments.size(); i++) {
                Type expectedTypeSignature = signature.getArgumentTypes().get(i);
                if (expectedTypeSignature instanceof FunctionType) {
                    continue;
                }
                Type actualTypeSignature = typeAnalyzer.getType(session, types, arguments.get(i));
                verifyTypeSignature(symbol, expectedTypeSignature, actualTypeSignature);
            }
        }

        private void verifyTypeSignature(Symbol symbol, Type expected, Type actual)
        {
            // UNKNOWN should be considered as a wildcard type, which matches all the other types
            if (!(actual instanceof UnknownType) && !typeCoercion.isTypeOnlyCoercion(actual, expected)) {
                checkArgument(expected.equals(actual), "type of symbol '%s' is expected to be %s, but the actual type is %s", symbol, expected, actual);
            }
        }
    }
}
