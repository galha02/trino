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

package io.prestosql.plugin.hive.coercions;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;

public class FloatToVarcharCoercer<F extends Type>
        extends TypeCoercer<F, VarcharType>
{
    public FloatToVarcharCoercer(F fromType, VarcharType toType)
    {
        super(fromType, toType);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        toType.writeSlice(blockBuilder, utf8Slice(String.valueOf(intBitsToFloat((int) REAL.getLong(block, position)))));
    }
}
