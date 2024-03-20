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
package io.trino.sql.planner.optimizations;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.DateTimes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionEquivalence
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final ExpressionEquivalence EQUIVALENCE = new ExpressionEquivalence(
            PLANNER_CONTEXT.getMetadata(),
            PLANNER_CONTEXT.getFunctionManager(),
            PLANNER_CONTEXT.getTypeManager(),
            new IrTypeAnalyzer(PLANNER_CONTEXT));

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MOD = FUNCTIONS.resolveFunction("mod", fromTypes(INTEGER, INTEGER));

    @Test
    public void testEquivalent()
    {
        assertEquivalent(
                new Constant(BIGINT, null),
                new Constant(BIGINT, null));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(DOUBLE, "b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "b_double"), new SymbolReference(BIGINT, "a_bigint")));
        assertEquivalent(
                TRUE_LITERAL,
                TRUE_LITERAL);
        assertEquivalent(
                new Constant(INTEGER, 4L),
                new Constant(INTEGER, 4L));
        assertEquivalent(
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))),
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))));
        assertEquivalent(
                new Constant(VARCHAR, Slices.utf8Slice("foo")),
                new Constant(VARCHAR, Slices.utf8Slice("foo")));

        assertEquivalent(
                new ComparisonExpression(EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))), new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5")))),
                new ComparisonExpression(EQUAL, new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5"))), new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4")))));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("foo")), new Constant(VARCHAR, Slices.utf8Slice("bar"))),
                new ComparisonExpression(EQUAL, new Constant(VARCHAR, Slices.utf8Slice("bar")), new Constant(VARCHAR, Slices.utf8Slice("foo"))));
        assertEquivalent(
                new ComparisonExpression(NOT_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(NOT_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-05-10 12:34:56.123456789")), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2021-05-10 12:34:56.123456789"))),
                new ComparisonExpression(EQUAL, new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2021-05-10 12:34:56.123456789")), new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2020-05-10 12:34:56.123456789"))));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 +8")), new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2021-05-10 12:34:56.123456789 +8"))),
                new ComparisonExpression(EQUAL, new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2021-05-10 12:34:56.123456789 +8")), new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 +8"))));

        assertEquivalent(
                new FunctionCall(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))),
                new FunctionCall(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))));

        assertEquivalent(
                new SymbolReference(BIGINT, "a_bigint"),
                new SymbolReference(BIGINT, "a_bigint"));
        assertEquivalent(
                new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")),
                new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "a_bigint")));
        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "a_bigint")));

        assertEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(DOUBLE, "b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "b_double"), new SymbolReference(BIGINT, "a_bigint")));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(TRUE_LITERAL, FALSE_LITERAL)),
                new LogicalExpression(AND, ImmutableList.of(FALSE_LITERAL, TRUE_LITERAL)));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "c_bigint"), new SymbolReference(BIGINT, "d_bigint")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "d_bigint"), new SymbolReference(BIGINT, "c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "a_bigint")))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "c_bigint"), new SymbolReference(BIGINT, "d_bigint")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "d_bigint"), new SymbolReference(BIGINT, "c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "a_bigint")))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 2L)))));

        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)))));
        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 2L), new Constant(INTEGER, 3L)), new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 4L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 3L), new Constant(INTEGER, 2L)))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "c_boolean"), new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "a_boolean"))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "b_boolean"))), new SymbolReference(BOOLEAN, "c_boolean"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "c_boolean"), new SymbolReference(BOOLEAN, "b_boolean"))), new SymbolReference(BOOLEAN, "a_boolean"))));
        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "c_boolean"), new SymbolReference(BOOLEAN, "b_boolean"))), new SymbolReference(BOOLEAN, "a_boolean"))));

        assertEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "d_boolean"), new SymbolReference(BOOLEAN, "e_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "f_boolean"), new SymbolReference(BOOLEAN, "g_boolean"), new SymbolReference(BOOLEAN, "h_boolean"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "h_boolean"), new SymbolReference(BOOLEAN, "g_boolean"), new SymbolReference(BOOLEAN, "f_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "e_boolean"), new SymbolReference(BOOLEAN, "d_boolean"))))));

        assertEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "d_boolean"), new SymbolReference(BOOLEAN, "e_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "f_boolean"), new SymbolReference(BOOLEAN, "g_boolean"), new SymbolReference(BOOLEAN, "h_boolean"))))),
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "h_boolean"), new SymbolReference(BOOLEAN, "g_boolean"), new SymbolReference(BOOLEAN, "f_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "b_boolean"), new SymbolReference(BOOLEAN, "a_boolean"), new SymbolReference(BOOLEAN, "c_boolean"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "e_boolean"), new SymbolReference(BOOLEAN, "d_boolean"))))));
    }

    private static void assertEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.copyOf(symbols.stream()
                .collect(toMap(identity(), TestExpressionEquivalence::generateType)));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, types))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", leftExpression, rightExpression))
                .isTrue();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, types))
                .describedAs(format("Expected (%s) and (%s) to be equivalent", rightExpression, leftExpression))
                .isTrue();
    }

    @Test
    public void testNotEquivalent()
    {
        assertNotEquivalent(
                new Constant(BOOLEAN, null),
                FALSE_LITERAL);
        assertNotEquivalent(
                FALSE_LITERAL,
                new Constant(BOOLEAN, null));
        assertNotEquivalent(
                TRUE_LITERAL,
                FALSE_LITERAL);
        assertNotEquivalent(
                new Constant(INTEGER, 4L),
                new Constant(INTEGER, 5L));
        assertNotEquivalent(
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("4.4"))),
                new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("5.5"))));
        assertNotEquivalent(
                new Constant(VARCHAR, Slices.utf8Slice("'foo'")),
                new Constant(VARCHAR, Slices.utf8Slice("'bar'")));

        assertNotEquivalent(
                new ComparisonExpression(EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new ComparisonExpression(NOT_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(NOT_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(IS_DISTINCT_FROM, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)),
                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)));

        assertNotEquivalent(
                new FunctionCall(MOD, ImmutableList.of(new Constant(INTEGER, 4L), new Constant(INTEGER, 5L))),
                new FunctionCall(MOD, ImmutableList.of(new Constant(INTEGER, 5L), new Constant(INTEGER, 4L))));

        assertNotEquivalent(
                new SymbolReference(BIGINT, "a_bigint"),
                new SymbolReference(BIGINT, "b_bigint"));
        assertNotEquivalent(
                new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")),
                new ComparisonExpression(EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "c_bigint")));
        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "c_bigint")));

        assertNotEquivalent(
                new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(DOUBLE, "b_double")),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "b_double"), new SymbolReference(BIGINT, "c_bigint")));

        assertNotEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)))));
        assertNotEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new Constant(INTEGER, 4L), new Constant(INTEGER, 5L)), new ComparisonExpression(LESS_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 7L)))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new Constant(INTEGER, 7L), new Constant(INTEGER, 6L)), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new Constant(INTEGER, 5L), new Constant(INTEGER, 6L)))));
        assertNotEquivalent(
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "c_bigint"), new SymbolReference(BIGINT, "d_bigint")))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "d_bigint"), new SymbolReference(BIGINT, "c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "c_bigint")))));
        assertNotEquivalent(
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BIGINT, "a_bigint"), new SymbolReference(BIGINT, "b_bigint")), new ComparisonExpression(LESS_THAN, new SymbolReference(BIGINT, "c_bigint"), new SymbolReference(BIGINT, "d_bigint")))),
                new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(BIGINT, "d_bigint"), new SymbolReference(BIGINT, "c_bigint")), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(BIGINT, "b_bigint"), new SymbolReference(BIGINT, "c_bigint")))));

        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(3), DateTimes.parseTimeWithTimeZone(3, "12:34:56.123 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(3), DateTimes.parseTimeWithTimeZone(3, "14:34:56.123 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(6), DateTimes.parseTimeWithTimeZone(6, "12:34:56.123456 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(6), DateTimes.parseTimeWithTimeZone(6, "14:34:56.123456 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(9), DateTimes.parseTimeWithTimeZone(9, "12:34:56.123456789 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(9), DateTimes.parseTimeWithTimeZone(9, "14:34:56.123456789 +02:00")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimeWithTimeZoneType(12), DateTimes.parseTimeWithTimeZone(12, "12:34:56.123456789012 +00:00")), VARCHAR),
                new Cast(new Constant(createTimeWithTimeZoneType(12), DateTimes.parseTimeWithTimeZone(12, "14:34:56.123456789012 +02:00")), VARCHAR));

        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(3), DateTimes.parseTimestampWithTimeZone(3, "2020-05-10 12:34:56.123 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(3), DateTimes.parseTimestampWithTimeZone(3, "2020-05-10 12:34:56.123 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(6), DateTimes.parseTimestampWithTimeZone(6, "2020-05-10 12:34:56.123456 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(6), DateTimes.parseTimestampWithTimeZone(6, "2020-05-10 12:34:56.123456 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(9), DateTimes.parseTimestampWithTimeZone(9, "2020-05-10 12:34:56.123456789 Europe/Paris")), VARCHAR));
        assertNotEquivalent(
                new Cast(new Constant(createTimestampWithTimeZoneType(12), DateTimes.parseTimestampWithTimeZone(12, "2020-05-10 12:34:56.123456789012 Europe/Warsaw")), VARCHAR),
                new Cast(new Constant(createTimestampWithTimeZoneType(12), DateTimes.parseTimestampWithTimeZone(12, "2020-05-10 12:34:56.123456789012 Europe/Paris")), VARCHAR));
    }

    private static void assertNotEquivalent(Expression leftExpression, Expression rightExpression)
    {
        Set<Symbol> symbols = extractUnique(ImmutableList.of(leftExpression, rightExpression));
        TypeProvider types = TypeProvider.copyOf(symbols.stream()
                .collect(toMap(identity(), TestExpressionEquivalence::generateType)));

        assertThat(areExpressionEquivalent(leftExpression, rightExpression, types))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", leftExpression, rightExpression))
                .isFalse();
        assertThat(areExpressionEquivalent(rightExpression, leftExpression, types))
                .describedAs(format("Expected (%s) and (%s) to not be equivalent", rightExpression, leftExpression))
                .isFalse();
    }

    private static boolean areExpressionEquivalent(Expression leftExpression, Expression rightExpression, TypeProvider types)
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(TEST_SESSION, transactionSession -> {
                    return EQUIVALENCE.areExpressionsEquivalent(transactionSession, leftExpression, rightExpression, types);
                });
    }

    private static Type generateType(Symbol symbol)
    {
        String typeName = Splitter.on('_').limit(2).splitToList(symbol.getName()).get(1);
        return PLANNER_CONTEXT.getTypeManager().getType(new TypeSignature(typeName, ImmutableList.of()));
    }
}
