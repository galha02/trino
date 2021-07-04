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
package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public class TinyintDecoder
        extends AbstractDecoder<Long>
{
    public TinyintDecoder()
    {
        super(TINYINT);
    }

    @Override
    public Long convert(String path, Object value)
    {
        long tinyint;
        if (value instanceof Number) {
            tinyint = ((Number) value).longValue();
        }
        else if (value instanceof String) {
            try {
                tinyint = Long.parseLong((String) value);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(TYPE_MISMATCH, format("Cannot parse value for field '%s' as TINYINT: %s", path, value));
            }
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a numeric value for field '%s' of type TINYINT: %s [%s]", path, value, value.getClass().getSimpleName()));
        }

        if (tinyint < Byte.MIN_VALUE || tinyint > Byte.MAX_VALUE) {
            throw new TrinoException(TYPE_MISMATCH, format("Value out of range for field '%s' of type TINYINT: %s", path, tinyint));
        }

        return tinyint;
    }

    @Override
    public void write(BlockBuilder output, Long value)
    {
        TINYINT.writeLong(output, value);
    }
}
