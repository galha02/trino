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
package io.prestosql.operator.scalar.timestamp;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

@ScalarOperator(CAST)
public final class TimestampToTimestampWithTimezoneCast
{
    private TimestampToTimestampWithTimezoneCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long cast(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("timestamp(p)") long value)
    {
        if (precision > 3) {
            value = scaleEpochMicrosToMillis(round(value, 3));
        }

        long millisUtc;
        if (session.isLegacyTimestamp()) {
            millisUtc = value;
        }
        else {
            ISOChronology localChronology = getChronology(session.getTimeZoneKey());
            // This cast does treat TIMESTAMP as wall time in session TZ. This means that in order to get
            // its UTC representation we need to shift the value by the offset of TZ.
            millisUtc = localChronology.getZone().convertLocalToUTC(value, false);
        }
        try {
            return packDateTimeWithZone(millisUtc, session.getTimeZoneKey());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Out of range for timestamp with time zone: " + millisUtc, e);
        }
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long cast(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return cast(6, session, timestamp.getEpochMicros());
    }
}
