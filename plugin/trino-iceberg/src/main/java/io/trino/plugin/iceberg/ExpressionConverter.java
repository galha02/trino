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
package io.trino.plugin.iceberg;

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Marker;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.expressions.Expression;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.predicate.Marker.Bound.ABOVE;
import static io.trino.spi.predicate.Marker.Bound.BELOW;
import static io.trino.spi.predicate.Marker.Bound.EXACTLY;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (tupleDomain.getDomains().isEmpty()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        Expression expression = alwaysTrue();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            expression = and(expression, toIcebergExpression(columnHandle.getName(), columnHandle.getType(), domain));
        }
        return expression;
    }

    private static Expression toIcebergExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        // Skip structural types. TODO: Evaluate Apache Iceberg's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return alwaysTrue();
        }

        ValueSet domainValues = domain.getValues();
        Expression expression = null;
        if (domain.isNullAllowed()) {
            expression = isNull(columnName);
        }

        if (domainValues instanceof SortedRangeSet) {
            List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
            expression = firstNonNull(expression, alwaysFalse());

            for (Range range : orderedRanges) {
                Marker low = range.getLow();
                Marker high = range.getHigh();
                Marker.Bound lowBound = low.getBound();
                Marker.Bound highBound = high.getBound();

                if (lowBound == EXACTLY && highBound == EXACTLY) {
                    // case ==
                    if (getIcebergLiteralValue(type, low).equals(getIcebergLiteralValue(type, high))) {
                        expression = or(expression, equal(columnName, getIcebergLiteralValue(type, low)));
                    }
                    else { // case between
                        Expression between = and(
                                greaterThanOrEqual(columnName, getIcebergLiteralValue(type, low)),
                                lessThanOrEqual(columnName, getIcebergLiteralValue(type, high)));
                        expression = or(expression, between);
                    }
                }
                else {
                    if (lowBound == EXACTLY && low.getValueBlock().isPresent()) {
                        // case >=
                        expression = or(expression, greaterThanOrEqual(columnName, getIcebergLiteralValue(type, low)));
                    }
                    else if (lowBound == ABOVE && low.getValueBlock().isPresent()) {
                        // case >
                        expression = or(expression, greaterThan(columnName, getIcebergLiteralValue(type, low)));
                    }

                    if (highBound == EXACTLY && high.getValueBlock().isPresent()) {
                        // case <=
                        if (low.getValueBlock().isPresent()) {
                            expression = and(expression, lessThanOrEqual(columnName, getIcebergLiteralValue(type, high)));
                        }
                        else {
                            expression = or(expression, lessThanOrEqual(columnName, getIcebergLiteralValue(type, high)));
                        }
                    }
                    else if (highBound == BELOW && high.getValueBlock().isPresent()) {
                        // case <
                        if (low.getValueBlock().isPresent()) {
                            expression = and(expression, lessThan(columnName, getIcebergLiteralValue(type, high)));
                        }
                        else {
                            expression = or(expression, lessThan(columnName, getIcebergLiteralValue(type, high)));
                        }
                    }
                }
            }
            return expression;
        }

        throw new VerifyException("Did not expect a domain value set other than SortedRangeSet but got " + domainValues.getClass().getSimpleName());
    }

    private static Object getIcebergLiteralValue(Type type, Marker marker)
    {
        if (type instanceof IntegerType) {
            return toIntExact((long) marker.getValue());
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) marker.getValue()));
        }

        // TODO: Remove this conversion once we move to next iceberg version
        if (type instanceof DateType) {
            return toIntExact(((Long) marker.getValue()));
        }

        if (type.equals(TIME_MICROS)) {
            return ((long) marker.getValue()) / PICOSECONDS_PER_MICROSECOND;
        }

        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros((LongTimestampWithTimeZone) marker.getValue());
        }

        if (type instanceof VarcharType) {
            return ((Slice) marker.getValue()).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) marker.getValue()).getBytes());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            Object value = requireNonNull(marker.getValue(), "The value of the marker must be non-null");
            if (Decimals.isShortDecimal(decimalType)) {
                return BigDecimal.valueOf((long) value).movePointLeft(decimalType.getScale());
            }
            return new BigDecimal(Decimals.decodeUnscaledValue((Slice) value), decimalType.getScale());
        }

        return marker.getValue();
    }
}
