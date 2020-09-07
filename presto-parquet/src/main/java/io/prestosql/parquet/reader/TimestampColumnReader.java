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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import static io.prestosql.parquet.ParquetTimestampUtils.getTimestampMillis;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;

public class TimestampColumnReader
        extends PrimitiveColumnReader
{
    private final DateTimeZone timeZone;

    public TimestampColumnReader(RichColumnDescriptor descriptor, DateTimeZone timeZone)
    {
        super(descriptor);
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long utcMillis = getTimestampMillis(valuesReader.readBytes());
            if (type instanceof TimestampWithTimeZoneType) {
                type.writeLong(blockBuilder, packDateTimeWithZone(utcMillis, UTC_KEY));
            }
            else {
                utcMillis = timeZone.convertUTCToLocal(utcMillis);
                type.writeLong(blockBuilder, utcMillis * MICROSECONDS_PER_MILLISECOND);
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readBytes();
        }
    }
}
