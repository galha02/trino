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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolToInputRewriter;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.SpecialForm.Form;
import io.prestosql.sql.relational.VariableReferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.OperatorSignatureUtils.mangleOperatorName;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.prestosql.sql.relational.SpecialForm.Form.AND;
import static io.prestosql.sql.relational.SpecialForm.Form.OR;
import static io.prestosql.sql.relational.SqlToRowExpressionTranslator.translate;
import static java.lang.Integer.min;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ExpressionEquivalence
{
    private static final Ordering<RowExpression> ROW_EXPRESSION_ORDERING = Ordering.from(new RowExpressionComparator());
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final CanonicalizationVisitor canonicalizationVisitor;

    public ExpressionEquivalence(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.canonicalizationVisitor = new CanonicalizationVisitor(metadata);
    }

    public boolean areExpressionsEquivalent(Session session, Expression leftExpression, Expression rightExpression, TypeProvider types)
    {
        Map<Symbol, Integer> symbolInput = new HashMap<>();
        Map<Integer, Type> inputTypes = new HashMap<>();
        int inputId = 0;
        for (Entry<Symbol, Type> entry : types.allTypes().entrySet()) {
            symbolInput.put(entry.getKey(), inputId);
            inputTypes.put(inputId, entry.getValue());
            inputId++;
        }
        RowExpression leftRowExpression = toRowExpression(session, leftExpression, symbolInput, inputTypes);
        RowExpression rightRowExpression = toRowExpression(session, rightExpression, symbolInput, inputTypes);

        RowExpression canonicalizedLeft = leftRowExpression.accept(canonicalizationVisitor, null);
        RowExpression canonicalizedRight = rightRowExpression.accept(canonicalizationVisitor, null);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    private RowExpression toRowExpression(Session session, Expression expression, Map<Symbol, Integer> symbolInput, Map<Integer, Type> inputTypes)
    {
        // replace qualified names with input references since row expressions do not support these
        Expression expressionWithInputReferences = new SymbolToInputRewriter(symbolInput).rewrite(expression);

        // determine the type of every expression
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(
                session,
                metadata,
                sqlParser,
                inputTypes,
                expressionWithInputReferences,
                emptyList(), /* parameters have already been replaced */
                WarningCollector.NOOP);

        // convert to row expression
        return translate(expressionWithInputReferences, SCALAR, expressionTypes, metadata.getFunctionManager(), metadata.getTypeManager(), session, false);
    }

    private static class CanonicalizationVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Metadata metadata;

        public CanonicalizationVisitor(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            call = new CallExpression(
                    call.getNameHint(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));

            String callName = call.getFunctionHandle().getSignature().getName();

            if (callName.equals(mangleOperatorName(EQUAL)) || callName.equals(mangleOperatorName(NOT_EQUAL)) || callName.equals(mangleOperatorName(IS_DISTINCT_FROM))) {
                // sort arguments
                return new CallExpression(
                        call.getNameHint(),
                        call.getFunctionHandle(),
                        call.getType(),
                        ROW_EXPRESSION_ORDERING.sortedCopy(call.getArguments()));
            }

            if (callName.equals(mangleOperatorName(GREATER_THAN)) || callName.equals(mangleOperatorName(GREATER_THAN_OR_EQUAL))) {
                // convert greater than to less than
                OperatorType newCallName = callName.equals(mangleOperatorName(GREATER_THAN)) ? LESS_THAN : LESS_THAN_OR_EQUAL;
                FunctionHandle newFunctionHandle = metadata.getFunctionManager().resolveOperator(
                        newCallName,
                        fromTypeSignatures(call.getFunctionHandle().getSignature().getArgumentTypes()));
                return new CallExpression(newCallName.name(), newFunctionHandle, call.getType(), swapPair(call.getArguments()));
            }

            return call;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            specialForm = new SpecialForm(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));

            if (specialForm.getForm() == AND || specialForm.getForm() == OR) {
                // if we have nested calls (of the same type) flatten them
                List<RowExpression> flattenedArguments = flattenNestedCallArgs(specialForm);

                // only consider distinct arguments
                Set<RowExpression> distinctArguments = ImmutableSet.copyOf(flattenedArguments);
                if (distinctArguments.size() == 1) {
                    return Iterables.getOnlyElement(distinctArguments);
                }

                // canonicalize the argument order (i.e., sort them)
                List<RowExpression> sortedArguments = ROW_EXPRESSION_ORDERING.sortedCopy(distinctArguments);

                return new SpecialForm(specialForm.getForm(), BOOLEAN, sortedArguments);
            }

            return specialForm;
        }

        public static List<RowExpression> flattenNestedCallArgs(SpecialForm specialForm)
        {
            Form form = specialForm.getForm();
            ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
            for (RowExpression argument : specialForm.getArguments()) {
                if (argument instanceof SpecialForm && form == ((SpecialForm) argument).getForm()) {
                    // same special form type, so flatten the args
                    newArguments.addAll(flattenNestedCallArgs((SpecialForm) argument));
                }
                else {
                    newArguments.add(argument);
                }
            }
            return newArguments.build();
        }

        @Override
        public RowExpression visitConstant(ConstantExpression constant, Void context)
        {
            return constant;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Void context)
        {
            return node;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }
    }

    private static class RowExpressionComparator
            implements Comparator<RowExpression>
    {
        private final Comparator<Object> classComparator = Ordering.arbitrary();
        private final ListComparator<RowExpression> argumentComparator = new ListComparator<>(this);

        @Override
        public int compare(RowExpression left, RowExpression right)
        {
            int result = classComparator.compare(left.getClass(), right.getClass());
            if (result != 0) {
                return result;
            }

            if (left instanceof CallExpression) {
                CallExpression leftCall = (CallExpression) left;
                CallExpression rightCall = (CallExpression) right;
                return ComparisonChain.start()
                        .compare(leftCall.getFunctionHandle().toString(), rightCall.getFunctionHandle().toString())
                        .compare(leftCall.getArguments(), rightCall.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof SpecialForm) {
                SpecialForm leftForm = (SpecialForm) left;
                SpecialForm rightForm = (SpecialForm) right;
                return ComparisonChain.start()
                        .compare(leftForm.getForm(), rightForm.getForm())
                        .compare(leftForm.getArguments(), rightForm.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof ConstantExpression) {
                ConstantExpression leftConstant = (ConstantExpression) left;
                ConstantExpression rightConstant = (ConstantExpression) right;

                result = leftConstant.getType().getTypeSignature().toString().compareTo(right.getType().getTypeSignature().toString());
                if (result != 0) {
                    return result;
                }

                Object leftValue = leftConstant.getValue();
                Object rightValue = rightConstant.getValue();

                if (leftValue == null) {
                    if (rightValue == null) {
                        return 0;
                    }
                    else {
                        return -1;
                    }
                }
                else if (rightValue == null) {
                    return 1;
                }

                Class<?> javaType = leftConstant.getType().getJavaType();
                if (javaType == boolean.class) {
                    return ((Boolean) leftValue).compareTo((Boolean) rightValue);
                }
                if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
                    return Long.compare(((Number) leftValue).longValue(), ((Number) rightValue).longValue());
                }
                if (javaType == float.class || javaType == double.class) {
                    return Double.compare(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue());
                }
                if (javaType == Slice.class) {
                    return ((Slice) leftValue).compareTo((Slice) rightValue);
                }

                // value is some random type (say regex), so we just randomly choose a greater value
                // todo: support all known type
                return -1;
            }

            if (left instanceof InputReferenceExpression) {
                return Integer.compare(((InputReferenceExpression) left).getField(), ((InputReferenceExpression) right).getField());
            }

            if (left instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression leftLambda = (LambdaDefinitionExpression) left;
                LambdaDefinitionExpression rightLambda = (LambdaDefinitionExpression) right;

                return ComparisonChain.start()
                        .compare(
                                leftLambda.getArgumentTypes(),
                                rightLambda.getArgumentTypes(),
                                new ListComparator<>(Comparator.comparing(Object::toString)))
                        .compare(
                                leftLambda.getArguments(),
                                rightLambda.getArguments(),
                                new ListComparator<>(Comparator.<String>naturalOrder()))
                        .compare(leftLambda.getBody(), rightLambda.getBody(), this)
                        .result();
            }

            if (left instanceof VariableReferenceExpression) {
                VariableReferenceExpression leftVariableReference = (VariableReferenceExpression) left;
                VariableReferenceExpression rightVariableReference = (VariableReferenceExpression) right;

                return ComparisonChain.start()
                        .compare(leftVariableReference.getName(), rightVariableReference.getName())
                        .compare(leftVariableReference.getType(), rightVariableReference.getType(), Comparator.comparing(Object::toString))
                        .result();
            }

            throw new IllegalArgumentException("Unsupported RowExpression type " + left.getClass().getSimpleName());
        }
    }

    private static class ListComparator<T>
            implements Comparator<List<T>>
    {
        private final Comparator<T> elementComparator;

        public ListComparator(Comparator<T> elementComparator)
        {
            this.elementComparator = requireNonNull(elementComparator, "elementComparator is null");
        }

        @Override
        public int compare(List<T> left, List<T> right)
        {
            int compareLength = min(left.size(), right.size());
            for (int i = 0; i < compareLength; i++) {
                int result = elementComparator.compare(left.get(i), right.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(left.size(), right.size());
        }
    }

    private static <T> List<T> swapPair(List<T> pair)
    {
        requireNonNull(pair, "pair is null");
        checkArgument(pair.size() == 2, "Expected pair to have two elements");
        return ImmutableList.of(pair.get(1), pair.get(0));
    }
}
