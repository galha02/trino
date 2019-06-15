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

import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.type.Re2JRegexpType.RE2J_REGEXP;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeCoercion
{
    private final Metadata metadata = createTestMetadataManager();
    private final TypeCoercion typeCoercion = new TypeCoercion(metadata::getType);

    @Test
    public void testIsTypeOnlyCoercion()
    {
        assertTrue(typeCoercion.isTypeOnlyCoercion(BIGINT, BIGINT));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("varchar(42)"), createType("varchar(44)")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("varchar(44)"), createType("varchar(42)")));

        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("char(42)"), createType("varchar(42)")));

        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("array(varchar(42))"), createType("array(varchar(44))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("array(varchar(44))"), createType("array(varchar(42))")));

        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("decimal(22,1)"), createType("decimal(23,1)")));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("decimal(2,1)"), createType("decimal(3,1)")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("decimal(23,1)"), createType("decimal(22,1)")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("decimal(3,1)"), createType("decimal(2,1)")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("decimal(3,1)"), createType("decimal(22,1)")));

        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("array(decimal(22,1))"), createType("array(decimal(23,1))")));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("array(decimal(2,1))"), createType("array(decimal(3,1))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("array(decimal(23,1))"), createType("array(decimal(22,1))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("array(decimal(3,1))"), createType("array(decimal(2,1))")));

        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("map(decimal(2,1), decimal(2,1))"), createType("map(decimal(2,1), decimal(3,1))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("map(decimal(2,1), decimal(2,1))"), createType("map(decimal(2,1), decimal(23,1))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("map(decimal(2,1), decimal(2,1))"), createType("map(decimal(2,1), decimal(3,2))")));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createType("map(decimal(22,1), decimal(2,1))"), createType("map(decimal(23,1), decimal(3,1))")));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createType("map(decimal(23,1), decimal(3,1))"), createType("map(decimal(22,1), decimal(2,1))")));
    }

    @Test
    public void testTypeCompatibility()
    {
        assertThat(UNKNOWN, UNKNOWN).hasCommonSuperType(UNKNOWN).canCoerceToEachOther();
        assertThat(BIGINT, BIGINT).hasCommonSuperType(BIGINT).canCoerceToEachOther();
        assertThat(UNKNOWN, BIGINT).hasCommonSuperType(BIGINT).canCoerceFirstToSecondOnly();

        assertThat(BIGINT, DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(DATE, TIMESTAMP).hasCommonSuperType(TIMESTAMP).canCoerceFirstToSecondOnly();
        assertThat(DATE, TIMESTAMP_WITH_TIME_ZONE).hasCommonSuperType(TIMESTAMP_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(TIME, TIME_WITH_TIME_ZONE).hasCommonSuperType(TIME_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE).hasCommonSuperType(TIMESTAMP_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, JONI_REGEXP).hasCommonSuperType(JONI_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, RE2J_REGEXP).hasCommonSuperType(RE2J_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, LIKE_PATTERN).hasCommonSuperType(LIKE_PATTERN).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, JSON_PATH).hasCommonSuperType(JSON_PATH).canCoerceFirstToSecondOnly();

        assertThat(REAL, DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(REAL, TINYINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, SMALLINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, INTEGER).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, BIGINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();

        assertThat(TIMESTAMP, TIME_WITH_TIME_ZONE).isIncompatible();
        assertThat(VARBINARY, VARCHAR).isIncompatible();

        assertThat("unknown", "array(bigint)").hasCommonSuperType("array(bigint)").canCoerceFirstToSecondOnly();
        assertThat("array(bigint)", "array(double)").hasCommonSuperType("array(double)").canCoerceFirstToSecondOnly();
        assertThat("array(bigint)", "array(unknown)").hasCommonSuperType("array(bigint)").canCoerceSecondToFirstOnly();
        assertThat("map(bigint,double)", "map(bigint,double)").hasCommonSuperType("map(bigint,double)").canCoerceToEachOther();
        assertThat("map(bigint,double)", "map(double,double)").hasCommonSuperType("map(double,double)").canCoerceFirstToSecondOnly();
        assertThat("row(a bigint,b double,c varchar)", "row(a bigint,b double,c varchar)").hasCommonSuperType("row(a bigint,b double,c varchar)").canCoerceToEachOther();

        assertThat("decimal(22,1)", "decimal(23,1)").hasCommonSuperType("decimal(23,1)").canCoerceFirstToSecondOnly();
        assertThat("bigint", "decimal(23,1)").hasCommonSuperType("decimal(23,1)").canCoerceFirstToSecondOnly();
        assertThat("bigint", "decimal(18,0)").hasCommonSuperType("decimal(19,0)").cannotCoerceToEachOther();
        assertThat("bigint", "decimal(19,0)").hasCommonSuperType("decimal(19,0)").canCoerceFirstToSecondOnly();
        assertThat("bigint", "decimal(37,1)").hasCommonSuperType("decimal(37,1)").canCoerceFirstToSecondOnly();
        assertThat("real", "decimal(37,1)").hasCommonSuperType("real").canCoerceSecondToFirstOnly();
        assertThat("array(decimal(23,1))", "array(decimal(22,1))").hasCommonSuperType("array(decimal(23,1))").canCoerceSecondToFirstOnly();
        assertThat("array(bigint)", "array(decimal(2,1))").hasCommonSuperType("array(decimal(20,1))").cannotCoerceToEachOther();
        assertThat("array(bigint)", "array(decimal(20,1))").hasCommonSuperType("array(decimal(20,1))").canCoerceFirstToSecondOnly();

        assertThat("decimal(3,2)", "double").hasCommonSuperType("double").canCoerceFirstToSecondOnly();
        assertThat("decimal(22,1)", "double").hasCommonSuperType("double").canCoerceFirstToSecondOnly();
        assertThat("decimal(37,1)", "double").hasCommonSuperType("double").canCoerceFirstToSecondOnly();
        assertThat("decimal(37,37)", "double").hasCommonSuperType("double").canCoerceFirstToSecondOnly();

        assertThat("decimal(22,1)", "real").hasCommonSuperType("real").canCoerceFirstToSecondOnly();
        assertThat("decimal(3,2)", "real").hasCommonSuperType("real").canCoerceFirstToSecondOnly();
        assertThat("decimal(37,37)", "real").hasCommonSuperType("real").canCoerceFirstToSecondOnly();

        assertThat("integer", "decimal(23,1)").hasCommonSuperType("decimal(23,1)").canCoerceFirstToSecondOnly();
        assertThat("integer", "decimal(9,0)").hasCommonSuperType("decimal(10,0)").cannotCoerceToEachOther();
        assertThat("integer", "decimal(10,0)").hasCommonSuperType("decimal(10,0)").canCoerceFirstToSecondOnly();
        assertThat("integer", "decimal(37,1)").hasCommonSuperType("decimal(37,1)").canCoerceFirstToSecondOnly();

        assertThat("tinyint", "decimal(2,0)").hasCommonSuperType("decimal(3,0)").cannotCoerceToEachOther();
        assertThat("tinyint", "decimal(9,0)").hasCommonSuperType("decimal(9,0)").canCoerceFirstToSecondOnly();
        assertThat("tinyint", "decimal(2,1)").hasCommonSuperType("decimal(4,1)").cannotCoerceToEachOther();
        assertThat("tinyint", "decimal(3,0)").hasCommonSuperType("decimal(3,0)").canCoerceFirstToSecondOnly();
        assertThat("tinyint", "decimal(37,1)").hasCommonSuperType("decimal(37,1)").canCoerceFirstToSecondOnly();

        assertThat("smallint", "decimal(37,1)").hasCommonSuperType("decimal(37,1)").canCoerceFirstToSecondOnly();
        assertThat("smallint", "decimal(4,0)").hasCommonSuperType("decimal(5,0)").cannotCoerceToEachOther();
        assertThat("smallint", "decimal(5,0)").hasCommonSuperType("decimal(5,0)").canCoerceFirstToSecondOnly();
        assertThat("smallint", "decimal(2,0)").hasCommonSuperType("decimal(5,0)").cannotCoerceToEachOther();
        assertThat("smallint", "decimal(9,0)").hasCommonSuperType("decimal(9,0)").canCoerceFirstToSecondOnly();
        assertThat("smallint", "decimal(2,1)").hasCommonSuperType("decimal(6,1)").cannotCoerceToEachOther();

        assertThat("char(42)", "char(40)").hasCommonSuperType("char(42)").canCoerceSecondToFirstOnly();
        assertThat("char(42)", "char(44)").hasCommonSuperType("char(44)").canCoerceFirstToSecondOnly();
        assertThat("varchar(42)", "varchar(42)").hasCommonSuperType("varchar(42)").canCoerceToEachOther();
        assertThat("varchar(42)", "varchar(44)").hasCommonSuperType("varchar(44)").canCoerceFirstToSecondOnly();
        assertThat("char(40)", "varchar(42)").hasCommonSuperType("char(42)").cannotCoerceToEachOther();
        assertThat("char(42)", "varchar(42)").hasCommonSuperType("char(42)").canCoerceSecondToFirstOnly();
        assertThat("char(44)", "varchar(42)").hasCommonSuperType("char(44)").canCoerceSecondToFirstOnly();

        assertThat(createType("char(42)"), JONI_REGEXP).hasCommonSuperType(JONI_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(createType("char(42)"), JSON_PATH).hasCommonSuperType(JSON_PATH).canCoerceFirstToSecondOnly();
        assertThat(createType("char(42)"), LIKE_PATTERN).hasCommonSuperType(LIKE_PATTERN).canCoerceFirstToSecondOnly();
        assertThat(createType("char(42)"), RE2J_REGEXP).hasCommonSuperType(RE2J_REGEXP).canCoerceFirstToSecondOnly();

        assertThat("row(varchar(2))", "row(varchar(5))").hasCommonSuperType("row(varchar(5))").canCoerceFirstToSecondOnly();

        assertThat("row(a integer)", "row(a bigint)").hasCommonSuperType("row(a bigint)").canCoerceFirstToSecondOnly();
        assertThat("row(a integer)", "row(b bigint)").hasCommonSuperType("row(bigint)").canCoerceFirstToSecondOnly();
        assertThat("row(integer)", "row(b bigint)").hasCommonSuperType("row(bigint)").canCoerceFirstToSecondOnly();
        assertThat("row(a integer)", "row(a varchar(2))").isIncompatible();
        assertThat("row(a integer)", "row(a integer,b varchar(2))").isIncompatible();
        assertThat("row(a integer,b varchar(2))", "row(a bigint,c varchar(5))").hasCommonSuperType("row(a bigint,varchar(5))").canCoerceFirstToSecondOnly();
        assertThat("row(a integer,b varchar(2))", "row(bigint,varchar(5))").hasCommonSuperType("row(bigint,varchar(5))").canCoerceFirstToSecondOnly();
        assertThat("row(a integer,b varchar(5))", "row(c bigint,d varchar(2))").hasCommonSuperType("row(bigint,varchar(5))").cannotCoerceToEachOther();
        assertThat("row(a row(c integer),b varchar(2))", "row(row(c integer),varchar(5))").hasCommonSuperType("row(row(c integer),varchar(5))").canCoerceFirstToSecondOnly();
        assertThat("row(a row(c integer),b varchar(2))", "row(a row(c integer),d varchar(5))").hasCommonSuperType("row(a row(c integer),varchar(5))").canCoerceFirstToSecondOnly();
        assertThat("row(a row(c integer),b varchar(5))", "row(d row(e integer),b varchar(5))").hasCommonSuperType("row(row(integer),b varchar(5))").canCoerceToEachOther();
    }

    @Test
    public void testCoerceTypeBase()
    {
        assertEquals(typeCoercion.coerceTypeBase(createDecimalType(21, 1), "decimal"), Optional.of(createDecimalType(21, 1)));
        assertEquals(typeCoercion.coerceTypeBase(BIGINT, "decimal"), Optional.of(createDecimalType(19, 0)));
        assertEquals(typeCoercion.coerceTypeBase(INTEGER, "decimal"), Optional.of(createDecimalType(10, 0)));
        assertEquals(typeCoercion.coerceTypeBase(TINYINT, "decimal"), Optional.of(createDecimalType(3, 0)));
        assertEquals(typeCoercion.coerceTypeBase(SMALLINT, "decimal"), Optional.of(createDecimalType(5, 0)));
    }

    @Test
    public void testCanCoerceIsTransitive()
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type transitiveType : types) {
            for (Type resultType : types) {
                if (typeCoercion.canCoerce(transitiveType, resultType)) {
                    for (Type sourceType : types) {
                        if (typeCoercion.canCoerce(sourceType, transitiveType)) {
                            if (!typeCoercion.canCoerce(sourceType, resultType)) {
                                fail(format("'%s' -> '%s' coercion is missing when transitive coercion is possible: '%s' -> '%s' -> '%s'",
                                        sourceType, resultType, sourceType, transitiveType, resultType));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCastOperatorsExistForCoercions()
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type sourceType : types) {
            for (Type resultType : types) {
                if (typeCoercion.canCoerce(sourceType, resultType) && sourceType != UNKNOWN && resultType != UNKNOWN) {
                    try {
                        metadata.getFunctionRegistry().getCoercion(sourceType.getTypeSignature(), resultType.getTypeSignature());
                    }
                    catch (Exception e) {
                        fail(format("'%s' -> '%s' coercion exists but there is no cast operator", sourceType, resultType), e);
                    }
                }
            }
        }
    }

    @Test
    public void testRowTypeCreation()
    {
        createType("row(time with time zone,time time with time zone)");
        createType("row(timestamp with time zone,\"timestamp\" timestamp with time zone)");
        createType("row(interval day to second,interval interval day to second)");
        createType("row(interval year to month,\"interval\" interval year to month)");
        createType("row(array(time with time zone),    \"a\" array(map(timestamp with time zone, interval day to second)))");
    }

    private Set<Type> getStandardPrimitiveTypes()
    {
        ImmutableSet.Builder<Type> builder = ImmutableSet.builder();
        // add unparametrized types
        builder.addAll(metadata.getTypes());
        // add corner cases for parametrized types
        builder.add(createDecimalType(1, 0));
        builder.add(createDecimalType(17, 0));
        builder.add(createDecimalType(38, 0));
        builder.add(createDecimalType(17, 17));
        builder.add(createDecimalType(38, 38));
        builder.add(createVarcharType(0));
        builder.add(createUnboundedVarcharType());
        builder.add(createCharType(0));
        builder.add(createCharType(42));
        return builder.build();
    }

    private CompatibilityAssertion assertThat(Type firstType, Type secondType)
    {
        Optional<Type> commonSuperType1 = typeCoercion.getCommonSuperType(firstType, secondType);
        Optional<Type> commonSuperType2 = typeCoercion.getCommonSuperType(secondType, firstType);
        assertEquals(commonSuperType1, commonSuperType2, "Expected getCommonSuperType to return the same result when invoked in either order");
        boolean canCoerceFirstToSecond = typeCoercion.canCoerce(firstType, secondType);
        boolean canCoerceSecondToFirst = typeCoercion.canCoerce(secondType, firstType);
        return new CompatibilityAssertion(commonSuperType1, canCoerceFirstToSecond, canCoerceSecondToFirst);
    }

    private CompatibilityAssertion assertThat(String firstType, String secondType)
    {
        return assertThat(createType(firstType), createType(secondType));
    }

    private Type createType(String signature)
    {
        return metadata.getType(parseTypeSignature(signature));
    }

    private class CompatibilityAssertion
    {
        private final Optional<Type> commonSuperType;
        private final boolean canCoerceFirstToSecond;
        private final boolean canCoerceSecondToFirst;

        public CompatibilityAssertion(Optional<Type> commonSuperType, boolean canCoerceFirstToSecond, boolean canCoerceSecondToFirst)
        {
            this.commonSuperType = requireNonNull(commonSuperType, "commonSuperType is null");

            // Assert that: (canFirstCoerceToSecond || canSecondCoerceToFirst) => commonSuperType.isPresent
            assertTrue(!(canCoerceFirstToSecond || canCoerceSecondToFirst) || commonSuperType.isPresent(), "Expected canCoercion to be false when there is no commonSuperType");
            this.canCoerceFirstToSecond = canCoerceFirstToSecond;
            this.canCoerceSecondToFirst = canCoerceSecondToFirst;
        }

        public void isIncompatible()
        {
            assertTrue(!commonSuperType.isPresent(), "Expected to be incompatible");
        }

        public CompatibilityAssertion hasCommonSuperType(Type expected)
        {
            assertTrue(commonSuperType.isPresent(), "Expected commonSuperType to be present");
            assertEquals(commonSuperType.get(), expected, "commonSuperType");
            return this;
        }

        public CompatibilityAssertion hasCommonSuperType(String expected)
        {
            return hasCommonSuperType(createType(expected));
        }

        public CompatibilityAssertion canCoerceToEachOther()
        {
            assertTrue(canCoerceFirstToSecond, "Expected first be coercible to second");
            assertTrue(canCoerceSecondToFirst, "Expected second be coercible to first");
            return this;
        }

        public CompatibilityAssertion canCoerceFirstToSecondOnly()
        {
            assertTrue(canCoerceFirstToSecond, "Expected first be coercible to second");
            assertFalse(canCoerceSecondToFirst, "Expected second NOT be coercible to first");
            return this;
        }

        public CompatibilityAssertion canCoerceSecondToFirstOnly()
        {
            assertFalse(canCoerceFirstToSecond, "Expected first NOT be coercible to second");
            assertTrue(canCoerceSecondToFirst, "Expected second be coercible to first");
            return this;
        }

        public CompatibilityAssertion cannotCoerceToEachOther()
        {
            assertFalse(canCoerceFirstToSecond, "Expected first NOT be coercible to second");
            assertFalse(canCoerceSecondToFirst, "Expected second NOT be coercible to first");
            return this;
        }
    }
}
