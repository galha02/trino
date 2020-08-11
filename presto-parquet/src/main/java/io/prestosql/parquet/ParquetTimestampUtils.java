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
package io.prestosql.parquet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TimestampType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;

import java.math.RoundingMode;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;

/**
 * Utility class for decoding INT96 encoded parquet timestamp to timestamp millis in GMT.
 * <p>
 */
public final class ParquetTimestampUtils
{
    @VisibleForTesting
    static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;

    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = MILLISECONDS.toNanos(1);

    private ParquetTimestampUtils() {}

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long getTimestampMillis(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            throw new PrestoException(NOT_SUPPORTED, "Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
        }
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private static long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    public static long scaleParquetTimestamp(OriginalType originalType, TimestampType prestoType, long parquetTimestampValue)
    {
        int parquetScale;
        if (originalType == TIMESTAMP_MILLIS) {
            parquetScale = 3;
        }
        else if (originalType == TIMESTAMP_MICROS) {
            parquetScale = 6;
        }
        else {
            throw new IllegalArgumentException("Unsupported original type: " + originalType);
        }

        int prestoPrecision = prestoType.getPrecision();
        if (prestoPrecision == parquetScale) {
            return parquetTimestampValue;
        }
        else if (prestoPrecision > parquetScale) {
            long scale = (long) Math.pow(10, prestoPrecision - parquetScale);
            return parquetTimestampValue * scale;
        }
        else {
            long scale = (long) Math.pow(10, parquetScale - prestoPrecision);
            return LongMath.divide(parquetTimestampValue, scale, RoundingMode.HALF_UP);
        }
    }
}
