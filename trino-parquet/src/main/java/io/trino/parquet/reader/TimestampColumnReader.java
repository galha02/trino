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
package io.trino.parquet.reader;

import io.trino.parquet.RichColumnDescriptor;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.plugin.base.type.PrestoTimestampEncoder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import static io.trino.parquet.ParquetTimestampUtils.decode;
import static io.trino.plugin.base.type.PrestoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
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

    // TODO: refactor to provide type at construction time (https://github.com/prestosql/presto/issues/5198)
    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (type instanceof TimestampWithTimeZoneType) {
                DecodedTimestamp decodedTimestamp = decode(valuesReader.readBytes());
                long utcMillis = decodedTimestamp.getEpochSeconds() * MILLISECONDS_PER_SECOND + decodedTimestamp.getNanosOfSecond() / NANOSECONDS_PER_MILLISECOND;
                type.writeLong(blockBuilder, packDateTimeWithZone(utcMillis, UTC_KEY));
            }
            else {
                PrestoTimestampEncoder<?> prestoTimestampEncoder = createTimestampEncoder((TimestampType) type, timeZone);
                prestoTimestampEncoder.write(decode(valuesReader.readBytes()), blockBuilder);
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
