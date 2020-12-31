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
package io.trino.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.util.Collections.emptyMap;

public class TestSqlToRowExpressionTranslator
{
    private final Metadata metadata = createTestMetadataManager();
    private final LiteralEncoder literalEncoder = new LiteralEncoder(metadata);

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        ImmutableMap.Builder<NodeRef<Expression>, Type> types = ImmutableMap.builder();
        types.put(NodeRef.of(expression), BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression, new LongLiteral("2"));
            types.put(NodeRef.of(expression), BIGINT);
        }
        translateAndOptimize(expression, types.build());
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertEquals(translateAndOptimize(expression("CAST(NULL AS DECIMAL(7,2))")), constant(null, createDecimalType(7, 2)));
        assertEquals(translateAndOptimize(expression("DECIMAL '42'")), constant(42L, createDecimalType(2, 0)));
        assertEquals(translateAndOptimize(expression("CAST(42 AS DECIMAL(7,2))")), constant(4200L, createDecimalType(7, 2)));
        assertEquals(translateAndOptimize(simplifyExpression(expression("CAST(42 AS DECIMAL(7,2))"))), constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertEquals(translateAndOptimize(expression("CAST(NULL AS DECIMAL(35,2))")), constant(null, createDecimalType(35, 2)));
        assertEquals(
                translateAndOptimize(expression("DECIMAL '123456789012345678901234567890'")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertEquals(
                translateAndOptimize(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
        assertEquals(
                translateAndOptimize(simplifyExpression(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))"))),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }

    private RowExpression translateAndOptimize(Expression expression)
    {
        return translateAndOptimize(expression, getExpressionTypes(expression));
    }

    private RowExpression translateAndOptimize(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata, TEST_SESSION, true);
    }

    private Expression simplifyExpression(Expression expression)
    {
        // Testing simplified expressions is important, since simplification may create CASTs or function calls that cannot be simplified by the ExpressionOptimizer

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(expression);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, TEST_SESSION, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return literalEncoder.toExpression(value, expressionTypes.get(NodeRef.of(expression)));
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata,
                new AllowAllAccessControl(),
                TEST_SESSION,
                TypeProvider.empty(),
                emptyMap(),
                node -> new IllegalStateException("Unexpected node: " + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}
