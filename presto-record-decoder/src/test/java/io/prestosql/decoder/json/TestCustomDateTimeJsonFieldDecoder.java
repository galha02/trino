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
package io.prestosql.decoder.json;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.decoder.DecoderTestColumnHandle;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCustomDateTimeJsonFieldDecoder
{
    private final JsonFieldDecoderTester timestampTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s");
    private final JsonFieldDecoderTester timestampWithTimeZoneTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd H:m:s Z");
    private final JsonFieldDecoderTester timeTester = new JsonFieldDecoderTester("custom-date-time", "mm:HH:ss");
    private final JsonFieldDecoderTester dateTester = new JsonFieldDecoderTester("custom-date-time", "MM/yyyy/dd");
    private final JsonFieldDecoderTester timeJustHourTester = new JsonFieldDecoderTester("custom-date-time", "HH");

    @Test
    public void testDecode()
    {
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", createTimestampType(0), 1519032011000L);
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", createTimestampType(0), 1519032011000L);
        timestampTester.assertDecodedAs("\"02/2018/19 9:20:11\"", createTimestampWithTimeZoneType(0), packDateTimeWithZone(1519032011000L, UTC_KEY));
        timestampWithTimeZoneTester.assertDecodedAs("\"02/2018/19 11:20:11 +02:00\"", createTimestampWithTimeZoneType(0), packDateTimeWithZone(1519032011000L, UTC_KEY)); // TODO: extract TZ from pattern
        timeTester.assertDecodedAs("\"15:13:18\"", TIME, 47718000);
        timeJustHourTester.assertDecodedAs("\"15\"", TIME, 54000000);
        timeJustHourTester.assertDecodedAs("15", TIME, 54000000);
        timeTester.assertDecodedAs("\"15:13:18\"", TIME_WITH_TIME_ZONE, packDateTimeWithZone(47718000, UTC_KEY));
        dateTester.assertDecodedAs("\"02/2018/11\"", DATE, 17573);
    }

    @Test
    public void testDecodeNulls()
    {
        dateTester.assertDecodedAsNull("null", DATE);
        dateTester.assertMissingDecodedAsNull(DATE);

        timeTester.assertDecodedAsNull("null", TIME);
        timeTester.assertMissingDecodedAsNull(TIME);

        timeTester.assertDecodedAsNull("null", TIME_WITH_TIME_ZONE);
        timeTester.assertMissingDecodedAsNull(TIME_WITH_TIME_ZONE);

        timestampTester.assertDecodedAsNull("null", createTimestampType(0));
        timestampTester.assertMissingDecodedAsNull(createTimestampType(0));

        timestampTester.assertDecodedAsNull("null", createTimestampWithTimeZoneType(0));
        timestampTester.assertMissingDecodedAsNull(createTimestampWithTimeZoneType(0));
    }

    @Test
    public void testDecodeInvalid()
    {
        timestampTester.assertInvalidInput("1", createTimestampType(0), "\\Qcould not parse value '1' as 'timestamp(0)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("{}", createTimestampType(0), "\\Qcould not parse non-value node as 'timestamp(0)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"a\"", createTimestampType(0), "\\Qcould not parse value 'a' as 'timestamp(0)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"15:13:18\"", createTimestampType(0), "\\Qcould not parse value '15:13:18' as 'timestamp(0)' for column 'some_column'\\E");
        timestampTester.assertInvalidInput("\"02/2018/11\"", createTimestampType(0), "\\Qcould not parse value '02/2018/11' as 'timestamp(0)' for column 'some_column'\\E");
    }

    @Test
    public void testInvalidFormatHint()
    {
        DecoderTestColumnHandle columnHandle = new DecoderTestColumnHandle(
                0,
                "some_column",
                createTimestampType(0),
                "mappedField",
                "custom-date-time",
                "XXMM/yyyy/dd H:m:sXX",
                false,
                false,
                false);
        assertThatThrownBy(() -> new JsonRowDecoderFactory(new ObjectMapperProvider().get()).create(emptyMap(), ImmutableSet.of(columnHandle)))
                .isInstanceOf(PrestoException.class)
                .hasMessageMatching("invalid joda pattern 'XXMM/yyyy/dd H:m:sXX' passed as format hint for column 'some_column'");
    }
}
