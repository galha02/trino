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
package io.trino.plugin.base.type;

import io.trino.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import static java.util.Objects.requireNonNull;

abstract class AbstractPrestoTimestampEncoder<T extends Comparable<T>>
        implements PrestoTimestampEncoder<T>
{
    protected final DateTimeZone timeZone;
    protected final TimestampType type;

    AbstractPrestoTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        this.type = requireNonNull(type, "type is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    @Override
    public TimestampType getType()
    {
        return type;
    }
}
