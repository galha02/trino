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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandlingStrategy;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.datatype.CreateAndInsertDataSetup;
import io.prestosql.tests.datatype.CreateAsSelectDataSetup;
import io.prestosql.tests.datatype.DataSetup;
import io.prestosql.tests.datatype.DataType;
import io.prestosql.tests.datatype.DataTypeTest;
import io.prestosql.tests.sql.JdbcSqlExecutor;
import io.prestosql.tests.sql.PrestoSqlExecutor;
import io.prestosql.tests.sql.TestTable;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.CharacterSemantics.BYTE;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.CharacterSemantics.CHAR;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.MAX_CHAR;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.MAX_NCHAR;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.MAX_NVARCHAR2;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.MAX_VARCHAR2;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.binaryDoubleDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.binaryFloatDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.blobDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.charDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.clobDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.doubleDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.integerDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.ncharDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.nclobDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.numberDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.nvarchar2DataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleFloatDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleTimestamp3DataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.oracleTimestamp3TimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.prestoTimestampDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.prestoTimestampTimeZoneDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.rawDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.realDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.tooLargeCharDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.tooLargeVarcharDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.unspecifiedNumberDataType;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.varchar2DataType;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.NUMBER_DEFAULT_SCALE;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.NUMBER_ROUNDING_MODE;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.basicCharacterTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.doubleTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.floatTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.getJvmTestTimeZone;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.numericTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.timestampTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.timestampWithTimeZoneTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.unboundedVarcharTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.unicodeTests;
import static com.starburstdata.presto.plugin.oracle.OracleTypeMappingData.varbinaryTests;
import static io.prestosql.plugin.jdbc.BaseJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING_STRATEGY;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandlingStrategy.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandlingStrategy.FAIL;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.tests.datatype.DataType.varbinaryDataType;
import static io.prestosql.tests.datatype.DataType.varcharDataType;
import static io.prestosql.tests.sql.TestTable.Type.TABLE;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestOracleTypeMapping
        extends AbstractTestQueryFramework
{
    public TestOracleTypeMapping()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .build(),
                Function.identity(),
                ImmutableList.of()));
    }

    @Test
    public void floatingPointMappings()
    {
        testTypeMapping("floats",
                floatTests(realDataType()),
                doubleTests(doubleDataType()));
    }

    @Test
    public void oracleFloatingPointMappings()
    {
        testTypeReadMapping("oracle_float",
                DataTypeTest.create()
                        .addRoundTrip(oracleFloatDataType(), 1e100d)
                        .addRoundTrip(oracleFloatDataType(), 1.0)
                        .addRoundTrip(oracleFloatDataType(), 123456.123456)
                        .addRoundTrip(oracleFloatDataType(), null)
                        .addRoundTrip(oracleFloatDataType(126), 1e100d)
                        .addRoundTrip(oracleFloatDataType(126), 1.0)
                        // to test the bounds of this type we would need to go via BigDecimal or String
                        .addRoundTrip(oracleFloatDataType(126), 1234567890123456789.0123456789)
                        .addRoundTrip(oracleFloatDataType(126), null)
                        .addRoundTrip(oracleFloatDataType(1), 100000.0)
                        .addRoundTrip(oracleFloatDataType(7), 123000.0));
    }

    @Test
    public void floatingPointReadMappings()
    {
        testTypeReadMapping("read_floats",
                floatTests(binaryFloatDataType()),
                doubleTests(binaryDoubleDataType()));
    }

    /* varchar tests */

    @Test
    public void varcharMapping()
    {
        testTypeMapping("varchar",
                basicCharacterTests(DataType::varcharDataType, MAX_VARCHAR2));
    }

    @Test
    public void varcharReadMapping()
    {
        testTypeReadMapping("read_varchar",
                basicCharacterTests(varchar2DataType(CHAR), MAX_VARCHAR2),
                basicCharacterTests(varchar2DataType(BYTE), MAX_VARCHAR2),
                basicCharacterTests(nvarchar2DataType(), MAX_NVARCHAR2));
    }

    /*
    The unicode tests assume the following Oracle database parameters:
     - NLS_NCHAR_CHARACTERSET = AL16UTF16
     - NLS_CHARACTERSET = AL32UTF8
     */
    @Test
    public void varcharUnicodeMapping()
    {
        testTypeMapping("varchar_unicode",
                unicodeTests(DataType::varcharDataType, codePoints(), MAX_VARCHAR2));
    }

    @Test
    public void varcharUnicodeReadMapping()
    {
        testTypeReadMapping("read_varchar_unicode",
                unicodeTests(varchar2DataType(CHAR), codePoints(), MAX_VARCHAR2),
                unicodeTests(varchar2DataType(BYTE), utf8Bytes(), MAX_VARCHAR2),
                unicodeTests(nvarchar2DataType(), String::length, MAX_NVARCHAR2));
    }

    @Test
    public void unboundedVarcharMapping()
    {
        testTypeMapping("unbounded",
                unboundedVarcharTests(varcharDataType()),
                unboundedVarcharTests(tooLargeVarcharDataType()),
                unboundedVarcharTests(tooLargeCharDataType()));
    }

    @Test
    public void unboundedVarcharReadMapping()
    {
        testTypeReadMapping("read_unbounded",
                unboundedVarcharTests(clobDataType()).addRoundTrip(clobDataType(), ""),
                unboundedVarcharTests(nclobDataType()).addRoundTrip(nclobDataType(), ""));
        // The tests on empty strings are read-only because Oracle treats empty
        // strings as NULL. The empty clob is generated by an Oracle function.
    }

    /* char tests */

    @Test
    public void charMapping()
    {
        testTypeMapping("char",
                basicCharacterTests(DataType::charDataType, MAX_CHAR));
    }

    @Test
    public void charReadMapping()
    {
        testTypeReadMapping("read_char",
                basicCharacterTests(charDataType(CHAR), MAX_CHAR),
                basicCharacterTests(charDataType(BYTE), MAX_CHAR),
                basicCharacterTests(ncharDataType(), MAX_NCHAR));
    }

    @Test
    public void charUnicodeMapping()
    {
        testTypeMapping("char_unicode",
                unicodeTests(DataType::charDataType, codePoints(), MAX_CHAR));
    }

    @Test
    public void charUnicodeReadMapping()
    {
        testTypeReadMapping("read_char_unicode",
                unicodeTests(charDataType(CHAR), codePoints(), MAX_CHAR),
                unicodeTests(charDataType(BYTE), utf8Bytes(), MAX_CHAR),
                unicodeTests(ncharDataType(), String::length, MAX_NCHAR));
    }

    /* Varbinary tests */

    @Test
    public void varbinaryMapping()
    {
        testTypeMapping("varbinary", varbinaryTests(varbinaryDataType()));
    }

    @Test
    public void varbinaryReadMapping()
    {
        testTypeReadMapping("read_varbinary",
                varbinaryTests(blobDataType())
                        .addRoundTrip(blobDataType(), new byte[] {}),
                varbinaryTests(rawDataType(2000)));
        // The test with the empty array is read-only because Oracle converts treats
        // binary data as NULL. The empty blob is generated by an Oracle function.
    }

    /* Decimal tests */

    @Test
    public void decimalMapping()
    {
        testTypeMapping("decimals", numericTests(DataType::decimalDataType));
    }

    @Test
    public void integerMappings()
    {
        testTypeMapping("integers",
                DataTypeTest.create()
                        .addRoundTrip(integerDataType("tinyint", 3), 0L)
                        .addRoundTrip(integerDataType("smallint", 5), 0L)
                        .addRoundTrip(integerDataType("integer", 10), 0L)
                        .addRoundTrip(integerDataType("bigint", 19), 0L));
    }

    @Test
    public void numberReadMapping()
    {
        testTypeReadMapping("read_decimals", numericTests(OracleTypeMappingData::oracleDecimalDataType));
    }

    @Test
    public void numberWithoutScaleReadMapping()
    {
        DataTypeTest.create()
                .addRoundTrip(numberDataType(1), BigDecimal.valueOf(1))
                .addRoundTrip(numberDataType(2), BigDecimal.valueOf(99))
                .addRoundTrip(numberDataType(38),
                        new BigDecimal("99999999999999999999999999999999999999")) // max
                .addRoundTrip(numberDataType(38),
                        new BigDecimal("-99999999999999999999999999999999999999")) // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_without_scale"));
    }

    @Test
    public void numberWithoutPrecisionAndScaleReadMapping()
    {
        DataTypeTest.create()
                .addRoundTrip(unspecifiedNumberDataType(9), BigDecimal.valueOf(1))
                .addRoundTrip(unspecifiedNumberDataType(9), BigDecimal.valueOf(99))
                .addRoundTrip(unspecifiedNumberDataType(9), new BigDecimal("9999999999999999999999999999.999999999")) // max
                .addRoundTrip(unspecifiedNumberDataType(9), new BigDecimal("-999999999999999999999999999.999999999")) // min
                .execute(getQueryRunner(), number(9), oracleCreateAndInsert("number_without_precision_and_scale"));
    }

    @Test
    public void roundingOfUnspecifiedNumber()
    {
        try (TestTable table = oracleTable("rounding", "col NUMBER", "(0.123456789)")) {
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), "Unsupported data type for column: COL");
            assertQuery(number(9), "SELECT * FROM " + table.getName(), "VALUES 0.123456789");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 0.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 0.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890.123456789)")) {
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), "Unsupported data type for column: COL");
            assertQueryFails(number(9), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQuery(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.12345679");
            assertQuery(number(HALF_EVEN, 6), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123457");
            assertQuery(number(HALF_EVEN, 3), "SELECT * FROM " + table.getName(), "VALUES 123456789012345678901234567890.123");
            assertQueryFails(number(UNNECESSARY, 3), "SELECT * FROM " + table.getName(), "Rounding necessary");
        }

        try (TestTable table = oracleTable("rounding", "col NUMBER", "(123456789012345678901234567890123456789)")) {
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), "Unsupported data type for column: COL");
            assertQueryFails(number(0), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 8), "SELECT * FROM " + table.getName(), "Decimal overflow");
            assertQueryFails(number(HALF_EVEN, 0), "SELECT * FROM " + table.getName(), "Decimal overflow");
        }
    }

    @Test
    public void numberNegativeScaleReadMapping()
    {
        // TODO: Add similar tests for write mappings.
        // Those tests would require the table to be created in Oracle, but values inserted
        // by Presto, which is outside the capabilities of the current DataSetup classes.
        DataTypeTest.create()
                .addRoundTrip(numberDataType(1, -1), BigDecimal.valueOf(2_0))
                .addRoundTrip(numberDataType(1, -1), BigDecimal.valueOf(3_5)) // More useful as a test for write mappings.
                .addRoundTrip(numberDataType(2, -4), BigDecimal.valueOf(47_0000))
                .addRoundTrip(numberDataType(2, -4), BigDecimal.valueOf(-8_0000))
                .addRoundTrip(numberDataType(8, -3), BigDecimal.valueOf(-88888888, -3))
                .addRoundTrip(numberDataType(8, -3), BigDecimal.valueOf(4050_000))
                .addRoundTrip(numberDataType(14, -14), BigDecimal.valueOf(14000014000014L, -14))
                .addRoundTrip(numberDataType(14, -14), BigDecimal.valueOf(1, -21))
                .addRoundTrip(numberDataType(5, -33), BigDecimal.valueOf(12345, -33))
                .addRoundTrip(numberDataType(5, -33), BigDecimal.valueOf(-12345, -33))
                .addRoundTrip(numberDataType(1, -37), BigDecimal.valueOf(1, -37))
                .addRoundTrip(numberDataType(1, -37), BigDecimal.valueOf(-1, -37))
                .addRoundTrip(numberDataType(37, -1),
                        new BigDecimal("99999999999999999999999999999999999990")) // max
                .addRoundTrip(numberDataType(37, -1),
                        new BigDecimal("-99999999999999999999999999999999999990")) // min
                .execute(getQueryRunner(), oracleCreateAndInsert("number_negative_s"));
    }

    @Test
    public void highNumberScale()
    {
        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 40)", "(0.0012345678901234567890123456789012345678)")) {
            String unsupported = "Unsupported data type for column: COL";
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), unsupported);
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), unsupported);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00123456789012345678901234567890123457");
            assertQuery(number(CONVERT_TO_VARCHAR), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012345678E-03'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 40)", "(0.0000000000000000000000123456789012345678)")) {
            String unsupported = "Unsupported data type for column: COL";
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), unsupported);
            assertQueryFails(number(UNNECESSARY), "SELECT * FROM " + table.getName(), unsupported);
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0.00000000000000000000001234567890123457");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(38, 80)", "(0.00000000000000000000000000000000000000000000012345678901234567890123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
            assertQuery(number(CONVERT_TO_VARCHAR), "SELECT * FROM " + table.getName(), "VALUES '1.2345678901234567890123456789012346E-46'");
        }
    }

    @Test
    public void numberWithHiveNegativeScaleReadMapping()
    {
        try (TestTable table = oracleTable("highNegativeNumberScale", "col NUMBER(38, -60)", "(1234567890123456789012345678901234567000000000000000000000000000000000000000000000000000000000000)")) {
            assertQueryFails(number(FAIL), "SELECT * FROM " + table.getName(), "Unsupported data type for column: COL");
            assertQuery(number(CONVERT_TO_VARCHAR), "SELECT * FROM " + table.getName(), "VALUES '1.234567890123456789012345678901234567E96'");
        }

        try (TestTable table = oracleTable("highNumberScale", "col NUMBER(18, 60)", "(0.000000000000000000000000000000000000000000000123456789012345678)")) {
            assertQuery(number(HALF_EVEN), "SELECT * FROM " + table.getName(), "VALUES 0");
        }
    }

    private Session number(int scale)
    {
        return number(FAIL, UNNECESSARY, Optional.of(scale));
    }

    private Session number(RoundingMode roundingMode)
    {
        return number(FAIL, roundingMode, Optional.empty());
    }

    private Session number(RoundingMode roundingMode, int scale)
    {
        return number(FAIL, roundingMode, Optional.of(scale));
    }

    private Session number(UnsupportedTypeHandlingStrategy unsupportedTypeHandlingStrategy)
    {
        return number(unsupportedTypeHandlingStrategy, UNNECESSARY, Optional.empty());
    }

    private Session number(UnsupportedTypeHandlingStrategy unsupportedTypeHandlingStrategy, RoundingMode roundingMode, Optional<Integer> scale)
    {
        Session.SessionBuilder builder = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", UNSUPPORTED_TYPE_HANDLING_STRATEGY, unsupportedTypeHandlingStrategy.name())
                .setCatalogSessionProperty("oracle", NUMBER_ROUNDING_MODE, roundingMode.name());
        if (scale.isPresent()) {
            builder.setCatalogSessionProperty("oracle", NUMBER_DEFAULT_SCALE, scale.get().toString());
        }
        return builder.build();
    }

    /* Datetime tests */

    @Test
    public void legacyDateMapping()
    {
        legacyDateTests(s -> prestoCreateAsSelect("legacy_date_in_" + s));
    }

    @Test
    public void legacyDateReadMapping()
    {
        legacyDateTests(s -> oracleCreateAndInsert("legacy_read_date_in_" + s));
    }

    void legacyDateTests(Function<String, DataSetup> dataSetup)
    {
        Map<String, TimeZoneKey> zonesBySqlName = ImmutableMap.of(
                "UTC", UTC_KEY,
                "JVM", getTimeZoneKey(ZoneId.systemDefault().getId()),
                "other", getTimeZoneKey(ZoneId.of("Europe/Vilnius").getId()));

        for (Map.Entry<String, TimeZoneKey> zone : zonesBySqlName.entrySet()) {
            runLegacyTimestampTestInZone(
                    dataSetup.apply(zone.getKey()),
                    zone.getValue().getId(),
                    OracleTypeMappingData.legacyDateTests());
        }
    }

    @Test
    public void nonLegacyDateMapping()
    {
        nonLegacyDateTests(s -> prestoCreateAsSelect("non_legacy_date_in_" + s));
    }

    @Test
    public void nonLegacyDateReadMapping()
    {
        nonLegacyDateTests(s -> oracleCreateAndInsert("non_legacy_read_date_in_" + s));
    }

    void nonLegacyDateTests(Function<String, DataSetup> dataSetup)
    {
        Map<String, TimeZoneKey> zonesBySqlName = ImmutableMap.of(
                "UTC", UTC_KEY,
                "JVM", getTimeZoneKey(ZoneId.systemDefault().getId()),
                "other", getTimeZoneKey(ZoneId.of("Europe/Vilnius").getId()));

        for (Map.Entry<String, TimeZoneKey> zone : zonesBySqlName.entrySet()) {
            runNonLegacyTimestampTestInZone(
                    dataSetup.apply(zone.getKey()),
                    zone.getValue().getId(),
                    OracleTypeMappingData.nonLegacyDateTests());
        }
    }

    @Test
    public void timestampMapping()
    {
        runTimestampTest(prestoCreateAsSelect("timestamp"),
                timestampTests(prestoTimestampDataType()));
    }

    @Test
    public void timestampReadMapping()
    {
        runTimestampTest(oracleCreateAndInsert("read_timestamp"),
                timestampTests(oracleTimestamp3DataType()));
    }

    @Test
    public void legacyTimestampReadMappingExtended()
    {
        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDateTime tsOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDateTime.parse("1983-10-01T00:00:00.000");
        verify(someZone.getRules().getValidOffsets(tsOfLocalTimeChangeBackwardAtMidnightInSomeZone.minusMinutes(1)).size() == 2);

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("1952-04-03T00:00:00.000")) // before epoch
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("1970-02-03T00:00:00.000"))
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("2017-07-01T00:00:00.000")) // summer on northern hemisphere (possible DST)
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("2017-01-01T00:00:00.000")) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(oracleTimestamp3DataType(), tsOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String zone : ImmutableList.of(UTC_KEY.getId(), getJvmTestTimeZone().getId(), someZone.getId())) {
            runLegacyTimestampTestInZone(oracleCreateAndInsert("legacy_read_timestamp_ext"), zone, testCases);
        }
    }

    @Test
    public void nonLegacyTimestampReadMappingExtended()
    {
        ZoneId jvmZone = getJvmTestTimeZone();

        LocalDateTime tsOfLocalTimeChangeForwardAtMidnightInJvmZone = LocalDateTime.parse("1970-01-01T00:00:00.000");
        verify(jvmZone.getRules().getValidOffsets(tsOfLocalTimeChangeForwardAtMidnightInJvmZone).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        LocalDateTime tsOfLocalTimeChangeForwardAtMidnightInSomeZone = LocalDateTime.parse("1983-04-01T00:00:00.000");
        verify(someZone.getRules().getValidOffsets(tsOfLocalTimeChangeForwardAtMidnightInSomeZone).isEmpty());
        LocalDateTime tsOfLocalTimeChangeBackwardAtMidnightInSomeZone = LocalDateTime.parse("1983-10-01T00:00:00.000");
        verify(someZone.getRules().getValidOffsets(tsOfLocalTimeChangeBackwardAtMidnightInSomeZone.minusMinutes(1)).size() == 2);

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("1952-04-03T00:00:00.000")) // before epoch
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("1970-01-01T00:00:00.000"))
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("1970-02-03T00:00:00.000"))
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("2017-07-01T00:00:00.000")) // summer on northern hemisphere (possible DST)
                .addRoundTrip(oracleTimestamp3DataType(), LocalDateTime.parse("2017-01-01T00:00:00.000")) // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip(oracleTimestamp3DataType(), tsOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(oracleTimestamp3DataType(), tsOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(oracleTimestamp3DataType(), tsOfLocalTimeChangeBackwardAtMidnightInSomeZone);

        for (String zone : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            runNonLegacyTimestampTestInZone(oracleCreateAndInsert("non_legacy_read_timestamp_ext"), zone, testCases);
        }
    }

    @Test
    public void timestampWithTimeZoneMapping()
    {
        runTimestampTest(prestoCreateAsSelect("timestamp_tz"),
                timestampWithTimeZoneTests(prestoTimestampTimeZoneDataType()));
    }

    @Test
    public void timestampWithTimeZoneReadMapping()
    {
        runTimestampTest(oracleCreateAndInsert("read_timestamp_tz"),
                timestampWithTimeZoneTests(oracleTimestamp3TimeZoneDataType()));
    }

    @Test
    public void timestampWithTimeZoneReadMappingExtended()
    {
        ZoneId jvmZone = getJvmTestTimeZone();

        verify(jvmZone.getRules().getValidOffsets(LocalDateTime.parse("1970-01-01T00:00:00.000")).isEmpty());
        ZonedDateTime tszOfLocalTimeChangeForwardAtMidnightInJvmZone = ZonedDateTime.parse("1970-01-01T01:00:00.000-07:00");

        ZoneId someZone = ZoneId.of("Europe/Vilnius");
        verify(someZone.getRules().getValidOffsets(LocalDateTime.parse("1983-04-01T00:00:00.000")).isEmpty());
        ZonedDateTime tszOfLocalTimeChangeForwardAtMidnightInSomeZone = ZonedDateTime.parse("1983-04-01T01:00:00.000+04:00");
        verify(someZone.getRules().getValidOffsets(LocalDateTime.parse("1983-10-01T00:00:00.000").minusMinutes(1)).size() == 2);
        ZonedDateTime tszOfLocalTimeChangeBackwardAtMidnightInSomeZone = ZonedDateTime.parse("1983-10-01T00:00:00.000+03:00");

        DataTypeTest testCases = DataTypeTest.create()
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), tszOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), tszOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), tszOfLocalTimeChangeBackwardAtMidnightInSomeZone)
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), ZonedDateTime.parse("1952-04-03T00:00:00.000+00:00")) // before epoch
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), ZonedDateTime.parse("1970-01-01T00:00:00.000+00:00"))
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), ZonedDateTime.parse("1970-02-03T00:00:00.000+00:00"))
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), ZonedDateTime.parse("2017-07-01T00:00:00.000+00:00"))
                .addRoundTrip(oracleTimestamp3TimeZoneDataType(), ZonedDateTime.parse("2017-01-01T00:00:00.000+00:00"));

        for (String zone : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), someZone.getId())) {
            runLegacyTimestampTestInZone(oracleCreateAndInsert("read_timestamp_tz_ext"), zone, testCases);
        }
    }

    /* Unsupported type tests */

    @Test
    public void unsupportedBasicType()
    {
        testUnsupportedOracleType("BFILE"); // Never in mapping
    }

    @Test
    public void unsupportedNumberScale()
    {
        // Difference between precision and negative scale greater than 38
        testUnsupportedOracleType("number(20, -20)");
        testUnsupportedOracleType("number(38, -84)");
        // Scale larger than precision.
        testUnsupportedOracleType("NUMBER(2, 4)"); // Explicitly removed from mapping
    }

    @Test
    public void unsupportedPrestoType()
    {
        assertQueryFails("CREATE TABLE unsupported_boolean AS " +
                        "SELECT true unsupported_type",
                "^Unsupported column type: boolean$");
    }

    /* Testing utilities */

    /**
     * Check that unsupported data types are ignored
     */
    protected void testUnsupportedOracleType(String dataTypeName)
    {
        try (TestTable table = new TestTable(getSqlExecutor(), "unsupported_type", format("(unsupported_type %s)", dataTypeName))) {
            assertQueryFails(number(FAIL), "DESCRIBE " + table.getName(), "Unsupported data type for column: UNSUPPORTED_TYPE");
        }
    }

    protected DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    protected DataSetup oracleCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(
                getSqlExecutor(),
                tableNamePrefix);
    }

    /**
     * Run {@link DataTypeTest}s, creating tables from Presto.
     */
    private void testTypeMapping(String tableNamePrefix, DataTypeTest... tests)
    {
        runTestsWithSetup(prestoCreateAsSelect(tableNamePrefix), tests);
    }

    /**
     * Run {@link DataTypeTest}s, creating tables with the JDBC.
     */
    private void testTypeReadMapping(String tableNamePrefix, DataTypeTest... tests)
    {
        runTestsWithSetup(oracleCreateAndInsert(tableNamePrefix), tests);
    }

    private void runTestsWithSetup(DataSetup dataSetup, DataTypeTest... tests)
    {
        for (DataTypeTest test : tests) {
            test.execute(getQueryRunner(), dataSetup);
        }
    }

    private void runTimestampTest(DataSetup dataSetup, DataTypeTest test)
    {
        runLegacyTimestampTestInZone(dataSetup, null, test);
    }

    /**
     * Run a {@link DataTypeTest} in the given time zone, using legacy timestamps.
     * <p>
     * If the given time zone is {@code null}, use the default session time zone.
     */
    private void runLegacyTimestampTestInZone(DataSetup dataSetup, String zone, DataTypeTest test)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession());
        if (zone != null) {
            session.setTimeZoneKey(getTimeZoneKey(zone));
        }
        test.execute(getQueryRunner(), session.build(), dataSetup);
    }

    /**
     * Run a {@link DataTypeTest} in the given time zone, using non-legacy timestamps.
     * <p>
     * If the given time zone is {@code null}, use the default session time zone.
     */
    private void runNonLegacyTimestampTestInZone(DataSetup dataSetup, String zone, DataTypeTest test)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SystemSessionProperties.LEGACY_TIMESTAMP, "false");
        if (zone != null) {
            session.setTimeZoneKey(getTimeZoneKey(zone));
        }
        test.execute(getQueryRunner(), session.build(), dataSetup);
    }

    private JdbcSqlExecutor getSqlExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", TestingOracleServer.USER);
        properties.setProperty("password", TestingOracleServer.PASSWORD);
        return new JdbcSqlExecutor(TestingOracleServer.getJdbcUrl(), properties);
    }

    private static ToIntFunction<String> codePoints()
    {
        return s -> s.codePointCount(0, s.length());
    }

    private static ToIntFunction<String> utf8Bytes()
    {
        return s -> s.getBytes(UTF_8).length;
    }

    private TestTable oracleTable(String tableName, String schema, String data)
    {
        return new TestTable(
                getSqlExecutor(),
                tableName,
                format("(%s)", schema),
                ImmutableList.of(data),
                TABLE);
    }
}
