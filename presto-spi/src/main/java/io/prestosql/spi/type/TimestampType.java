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
package io.prestosql.spi.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;

import static java.lang.String.format;

/**
 * A timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC and is to be interpreted as date-time in UTC.
 * In legacy timestamp semantics, timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC and is to be
 * interpreted in session time zone.
 */
public final class TimestampType
        extends AbstractLongType
{
    /**
     * @deprecated Use {@link #createTimestampType(int)} instead.
     */
    @Deprecated
    public static final TimestampType TIMESTAMP = new TimestampType();

    public static TimestampType createTimestampType(int precision)
    {
        if (precision != 3) {
            throw new IllegalArgumentException(format("Precision %s is not supported", precision));
        }
        return TIMESTAMP;
    }

    private TimestampType()
    {
        super(new TypeSignature(StandardTypes.TIMESTAMP));
    }

    public int getPrecision()
    {
        return 3;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (session.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position, 0), session.getTimeZoneKey());
        }
        else {
            return new SqlTimestamp(block.getLong(position, 0));
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIMESTAMP;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
