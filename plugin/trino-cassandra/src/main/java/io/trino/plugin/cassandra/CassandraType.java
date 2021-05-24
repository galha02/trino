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
package io.trino.plugin.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteralForJson;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public class CassandraType
{
    public enum Kind
    {
        BOOLEAN,
        TINYINT,
        SMALLINT,
        INT,
        BIGINT,
        FLOAT,
        DOUBLE,
        DECIMAL,
        DATE,
        TIMESTAMP,
        ASCII,
        TEXT,
        VARCHAR,
        BLOB,
        UUID,
        TIMEUUID,
        COUNTER,
        VARINT,
        INET,
        CUSTOM,
        LIST,
        SET,
        MAP,
        /**/;
    }

    private final Kind kind;
    private final Type trinoType;

    @JsonCreator
    public CassandraType(
            @JsonProperty("kind") Kind kind,
            @JsonProperty("trinoType") Type trinoType)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.trinoType = requireNonNull(trinoType, "trinoType is null");
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    @JsonProperty
    public Type getTrinoType()
    {
        return trinoType;
    }

    public String getName()
    {
        return kind.name();
    }

    public static Optional<CassandraType> toCassandraType(DataType dataType)
    {
        switch (dataType.getName()) {
            case ASCII:
                return Optional.of(CassandraTypes.ASCII);
            case BIGINT:
                return Optional.of(CassandraTypes.BIGINT);
            case BLOB:
                return Optional.of(CassandraTypes.BLOB);
            case BOOLEAN:
                return Optional.of(CassandraTypes.BOOLEAN);
            case COUNTER:
                return Optional.of(CassandraTypes.COUNTER);
            case CUSTOM:
                return Optional.of(CassandraTypes.CUSTOM);
            case DATE:
                return Optional.of(CassandraTypes.DATE);
            case DECIMAL:
                return Optional.of(CassandraTypes.DECIMAL);
            case DOUBLE:
                return Optional.of(CassandraTypes.DOUBLE);
            case FLOAT:
                return Optional.of(CassandraTypes.FLOAT);
            case INET:
                return Optional.of(CassandraTypes.INET);
            case INT:
                return Optional.of(CassandraTypes.INT);
            case LIST:
                return Optional.of(CassandraTypes.LIST);
            case MAP:
                return Optional.of(CassandraTypes.MAP);
            case SET:
                return Optional.of(CassandraTypes.SET);
            case SMALLINT:
                return Optional.of(CassandraTypes.SMALLINT);
            case TEXT:
                return Optional.of(CassandraTypes.TEXT);
            case TIMESTAMP:
                return Optional.of(CassandraTypes.TIMESTAMP);
            case TIMEUUID:
                return Optional.of(CassandraTypes.TIMEUUID);
            case TINYINT:
                return Optional.of(CassandraTypes.TINYINT);
            case UUID:
                return Optional.of(CassandraTypes.UUID);
            case VARCHAR:
                return Optional.of(CassandraTypes.VARCHAR);
            case VARINT:
                return Optional.of(CassandraTypes.VARINT);
            default:
                return Optional.empty();
        }
    }

    public NullableValue getColumnValue(Row row, int position)
    {
        if (row.isNull(position)) {
            return NullableValue.asNull(trinoType);
        }

        switch (this.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(trinoType, utf8Slice(row.getString(position)));
            case INT:
                return NullableValue.of(trinoType, (long) row.getInt(position));
            case SMALLINT:
                return NullableValue.of(trinoType, (long) row.getShort(position));
            case TINYINT:
                return NullableValue.of(trinoType, (long) row.getByte(position));
            case BIGINT:
            case COUNTER:
                return NullableValue.of(trinoType, row.getLong(position));
            case BOOLEAN:
                return NullableValue.of(trinoType, row.getBool(position));
            case DOUBLE:
                return NullableValue.of(trinoType, row.getDouble(position));
            case FLOAT:
                return NullableValue.of(trinoType, (long) floatToRawIntBits(row.getFloat(position)));
            case DECIMAL:
                return NullableValue.of(trinoType, row.getDecimal(position).doubleValue());
            case UUID:
            case TIMEUUID:
                return NullableValue.of(trinoType, utf8Slice(row.getUUID(position).toString()));
            case TIMESTAMP:
                return NullableValue.of(trinoType, packDateTimeWithZone(row.getTimestamp(position).getTime(), TimeZoneKey.UTC_KEY));
            case DATE:
                return NullableValue.of(trinoType, (long) row.getDate(position).getDaysSinceEpoch());
            case INET:
                return NullableValue.of(trinoType, utf8Slice(toAddrString(row.getInet(position))));
            case VARINT:
                return NullableValue.of(trinoType, utf8Slice(row.getVarint(position).toString()));
            case BLOB:
            case CUSTOM:
                return NullableValue.of(trinoType, wrappedBuffer(row.getBytesUnsafe(position)));
            case SET:
            case LIST:
                return NullableValue.of(trinoType, utf8Slice(buildArrayValue(row, position)));
            case MAP:
                return NullableValue.of(trinoType, utf8Slice(buildMapValue(row, position)));
        }
        throw new IllegalStateException("Handling of type " + this + " is not implemented");
    }

    private static String buildMapValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        checkArgument(type.getTypeArguments().size() == 2, "Expected two type arguments, got: %s", type.getTypeArguments());
        DataType keyType = type.getTypeArguments().get(0);
        DataType valueType = type.getTypeArguments().get(1);
        return buildMapValue((Map<?, ?>) row.getObject(position), keyType, valueType);
    }

    private static String buildMapValue(Map<?, ?> cassandraMap, DataType keyType, DataType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : cassandraMap.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToJson(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToJson(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String buildArrayValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        DataType elementType = getOnlyElement(type.getTypeArguments());
        return buildArrayValue((Collection<?>) row.getObject(position), elementType);
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> cassandraCollection, DataType elementType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : cassandraCollection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToJson(value, elementType));
        }
        sb.append("]");
        return sb.toString();
    }

    // TODO unify with toCqlLiteral
    public String getColumnValueForCql(Row row, int position)
    {
        if (row.isNull(position)) {
            return null;
        }

        switch (this.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return quoteStringLiteral(row.getString(position));
            case INT:
                return Integer.toString(row.getInt(position));
            case SMALLINT:
                return Short.toString(row.getShort(position));
            case TINYINT:
                return Byte.toString(row.getByte(position));
            case BIGINT:
            case COUNTER:
                return Long.toString(row.getLong(position));
            case BOOLEAN:
                return Boolean.toString(row.getBool(position));
            case DOUBLE:
                return Double.toString(row.getDouble(position));
            case FLOAT:
                return Float.toString(row.getFloat(position));
            case DECIMAL:
                return row.getDecimal(position).toString();
            case UUID:
            case TIMEUUID:
                return row.getUUID(position).toString();
            case TIMESTAMP:
                return Long.toString(row.getTimestamp(position).getTime());
            case DATE:
                return row.getDate(position).toString();
            case INET:
                return quoteStringLiteral(toAddrString(row.getInet(position)));
            case VARINT:
                return row.getVarint(position).toString();
            case BLOB:
            case CUSTOM:
                return Bytes.toHexString(row.getBytesUnsafe(position));

            case LIST:
            case SET:
            case MAP:
                // unsupported
                break;
        }
        throw new IllegalStateException("Handling of type " + this + " is not implemented");
    }

    // TODO unify with getColumnValueForCql
    public String toCqlLiteral(Object trinoNativeValue)
    {
        if (this.kind == Kind.TIMESTAMP) {
            return String.valueOf(unpackMillisUtc((Long) trinoNativeValue));
        }

        String value;
        if (trinoNativeValue instanceof Slice) {
            value = ((Slice) trinoNativeValue).toStringUtf8();
        }
        else {
            value = trinoNativeValue.toString();
        }

        switch (this.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return quoteStringLiteral(value);
            case INET:
                // remove '/' in the string. e.g. /127.0.0.1
                return quoteStringLiteral(value.substring(1));
            default:
                return value;
        }
    }

    private static String objectToJson(Object cassandraValue, DataType dataType)
    {
        CassandraType cassandraType = toCassandraType(dataType)
                .orElseThrow(() -> new IllegalStateException("Unsupported type: " + dataType));

        switch (cassandraType.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case TIMESTAMP:
            case DATE:
            case INET:
            case VARINT:
                return quoteStringLiteralForJson(cassandraValue.toString());

            case BLOB:
            case CUSTOM:
                return quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) cassandraValue));

            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return cassandraValue.toString();
            case LIST:
            case SET:
                return buildArrayValue((Collection<?>) cassandraValue, getOnlyElement(dataType.getTypeArguments()));
            case MAP:
                return buildMapValue((Map<?, ?>) cassandraValue, dataType.getTypeArguments().get(0), dataType.getTypeArguments().get(1));
        }
        throw new IllegalStateException("Unsupported type: " + cassandraType);
    }

    public Object getJavaValue(Object trinoNativeValue)
    {
        switch (this.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return ((Slice) trinoNativeValue).toStringUtf8();
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return trinoNativeValue;
            case INET:
                return InetAddresses.forString(((Slice) trinoNativeValue).toStringUtf8());
            case INT:
            case SMALLINT:
            case TINYINT:
                return ((Long) trinoNativeValue).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return intBitsToFloat(((Long) trinoNativeValue).intValue());
            case DECIMAL:
                // conversion can result in precision lost
                // Trino uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(trinoNativeValue.toString());
            case TIMESTAMP:
                return new Date(unpackMillisUtc((Long) trinoNativeValue));
            case DATE:
                return LocalDate.fromDaysSinceEpoch(((Long) trinoNativeValue).intValue());
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) trinoNativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
                return ((Slice) trinoNativeValue).toStringUtf8();
            case VARINT:
                return new BigInteger(((Slice) trinoNativeValue).toStringUtf8());
            case SET:
            case LIST:
            case MAP:
        }
        throw new IllegalStateException("Back conversion not implemented for " + this);
    }

    public boolean isSupportedPartitionKey()
    {
        switch (this.kind) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case UUID:
            case TIMEUUID:
                return true;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                return false;
        }
    }

    public static boolean isFullySupported(DataType dataType)
    {
        if (toCassandraType(dataType).isEmpty()) {
            return false;
        }

        return dataType.getTypeArguments().stream()
                .allMatch(CassandraType::isFullySupported);
    }

    public static CassandraType toCassandraType(Type type, ProtocolVersion protocolVersion)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return CassandraTypes.BOOLEAN;
        }
        if (type.equals(BigintType.BIGINT)) {
            return CassandraTypes.BIGINT;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return CassandraTypes.INT;
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return CassandraTypes.SMALLINT;
        }
        if (type.equals(TinyintType.TINYINT)) {
            return CassandraTypes.TINYINT;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return CassandraTypes.DOUBLE;
        }
        if (type.equals(RealType.REAL)) {
            return CassandraTypes.FLOAT;
        }
        if (type instanceof VarcharType) {
            return CassandraTypes.TEXT;
        }
        if (type.equals(DateType.DATE)) {
            return protocolVersion.toInt() <= ProtocolVersion.V3.toInt()
                    ? CassandraTypes.TEXT
                    : CassandraTypes.DATE;
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return CassandraTypes.BLOB;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return CassandraTypes.TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraType that = (CassandraType) o;
        return kind == that.kind && Objects.equals(trinoType, that.trinoType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, trinoType);
    }
}
