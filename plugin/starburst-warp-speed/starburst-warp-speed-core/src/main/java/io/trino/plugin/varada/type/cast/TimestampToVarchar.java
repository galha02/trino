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
package io.trino.plugin.varada.type.cast;

import io.airlift.slice.Slice;
import io.trino.spi.type.LongTimestamp;

import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static java.time.ZoneOffset.UTC;

public final class TimestampToVarchar
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private TimestampToVarchar() {}

    public static Slice cast(long precision, long epochMicros)
    {
        return utf8Slice(DateTimes.formatTimestamp((int) precision, epochMicros, 0, UTC, TIMESTAMP_FORMATTER));
    }

    public static Slice cast(long precision, LongTimestamp timestamp)
    {
        return utf8Slice(DateTimes.formatTimestamp((int) precision, timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), UTC, TIMESTAMP_FORMATTER));
    }
}