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
package io.trino.plugin.pinot.decoders;

import io.prestosql.spi.block.BlockBuilder;

import java.util.function.Supplier;

import static java.lang.Float.floatToIntBits;

public class RealDecoder
        implements Decoder
{
    @Override
    public void decode(Supplier<Object> getter, BlockBuilder output)
    {
        output.writeInt(floatToIntBits(((Number) getter.get()).floatValue()));
    }
}
