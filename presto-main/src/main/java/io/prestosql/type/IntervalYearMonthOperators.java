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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.client.IntervalYearMonth;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.DIVIDE;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.NEGATION;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class IntervalYearMonthOperators
{
    private IntervalYearMonthOperators() {}

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long add(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left + right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long subtract(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left - right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByBigint(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long multiplyByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        if (Double.isNaN(right)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return (long) (left * right);
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long bigintMultiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left * right;
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long doubleMultiply(@SqlType(StandardTypes.DOUBLE) double left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        if (Double.isNaN(left)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot multiply by double NaN");
        }
        return (long) (left * right);
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long divideByDouble(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        if (Double.isNaN(right) || right == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot divide by double %s", right));
        }
        return (long) (left / right);
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH)
    public static long negate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return -value;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left >= right;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        return utf8Slice(IntervalYearMonth.formatMonths(toIntExact(value)));
    }
}
