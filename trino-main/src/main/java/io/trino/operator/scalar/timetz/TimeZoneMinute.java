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
package io.trino.operator.scalar.timetz;

import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.type.DateTimes.MINUTES_PER_HOUR;

@Description("Time zone minute of the given time")
@ScalarFunction("timezone_minute")
public final class TimeZoneMinute
{
    private TimeZoneMinute() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("time(p) with time zone") long packedTime)
    {
        return unpackOffsetMinutes(packedTime) % MINUTES_PER_HOUR;
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("time(p) with time zone") LongTimeWithTimeZone time)
    {
        return time.getOffsetMinutes() % MINUTES_PER_HOUR;
    }
}
