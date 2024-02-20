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
package io.trino.plugin.accumulo.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.GregorianCalendar;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestField
{
    @Test
    public void testTypeIsNull()
    {
        assertThatThrownBy(() -> new Field(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
    }

    @Test
    public void testArray()
    {
        Type type = new ArrayType(VARCHAR);
        Block expected = AccumuloRowSerializer.getBlockFromArray(VARCHAR, ImmutableList.of("a", "b", "c"));
        Field f1 = new Field(expected, type);
        assertThat(f1.getArray()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testBoolean()
    {
        Type type = BOOLEAN;
        Field f1 = new Field(true, type);
        assertThat(f1.getBoolean().booleanValue()).isEqualTo(true);
        assertThat(f1.getObject()).isEqualTo(true);
        assertThat(f1.getType()).isEqualTo(type);

        f1 = new Field(false, type);
        assertThat(f1.getBoolean().booleanValue()).isEqualTo(false);
        assertThat(f1.getObject()).isEqualTo(false);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testDate()
    {
        Type type = DATE;
        long expected = LocalDate.parse("1999-01-01").toEpochDay();
        Field f1 = new Field(10592L, type);
        assertThat(f1.getDate()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testDouble()
    {
        Type type = DOUBLE;
        Double expected = 123.45678;
        Field f1 = new Field(expected, type);
        assertThat(f1.getDouble()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testFloat()
    {
        Type type = REAL;
        Float expected = 123.45678f;
        Field f1 = new Field((long) floatToIntBits(expected), type);
        assertThat(f1.getFloat()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testInt()
    {
        Type type = INTEGER;
        Integer expected = 12345678;
        Field f1 = new Field((long) expected, type);
        assertThat(f1.getInt()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testLong()
    {
        Type type = BIGINT;
        Long expected = 12345678L;
        Field f1 = new Field(expected, type);
        assertThat(f1.getLong()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testMap()
    {
        Type type = TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature()),
                TypeSignatureParameter.typeParameter(BIGINT.getTypeSignature())));
        SqlMap expected = AccumuloRowSerializer.getSqlMapFromMap(type, ImmutableMap.of("a", 1L, "b", 2L, "c", 3L));
        Field f1 = new Field(expected, type);
        assertThat(f1.getMap()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testSmallInt()
    {
        Type type = SMALLINT;
        Short expected = 12345;
        Field f1 = new Field((long) expected, type);
        assertThat(f1.getShort()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testTime()
    {
        Type type = TIME_MILLIS;
        Time expected = new Time(new GregorianCalendar(1970, 0, 1, 12, 30, 0).getTime().getTime());
        Field f1 = new Field(70200000L, type);
        assertThat(f1.getTime()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testTimestamp()
    {
        Type type = TIMESTAMP_MILLIS;
        Timestamp expected = new Timestamp(new GregorianCalendar(1999, 0, 1, 12, 30, 0).getTime().getTime());
        Field f1 = new Field(915_219_000_000_000L, type);
        assertThat(f1.getTimestamp()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testTinyInt()
    {
        Type type = TINYINT;
        Byte expected = 123;
        Field f1 = new Field((long) expected, type);
        assertThat(f1.getByte()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testVarbinary()
    {
        Type type = VARBINARY;
        byte[] expected = "O'Leary".getBytes(UTF_8);
        Field f1 = new Field(Slices.wrappedBuffer(expected.clone()), type);
        assertThat(f1.getVarbinary()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }

    @Test
    public void testVarchar()
    {
        Type type = VARCHAR;
        String expected = "O'Leary";
        Field f1 = new Field(utf8Slice(expected), type);
        assertThat(f1.getVarchar()).isEqualTo(expected);
        assertThat(f1.getObject()).isEqualTo(expected);
        assertThat(f1.getType()).isEqualTo(type);
    }
}
