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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;

import java.util.Optional;

public class IntColumnAdapter
        implements ColumnAdapter<int[]>
{
    public static final IntColumnAdapter INT_ADAPTER = new IntColumnAdapter();

    @Override
    public int[] createBuffer(int size)
    {
        return new int[size];
    }

    @Override
    public Block createNonNullBlock(int size, int[] values)
    {
        return new IntArrayBlock(size, Optional.empty(), values);
    }

    @Override
    public Block createNullableBlock(int size, boolean[] nulls, int[] values)
    {
        return new IntArrayBlock(size, Optional.of(nulls), values);
    }

    @Override
    public void copyValue(int[] source, int sourceIndex, int[] destination, int destinationIndex)
    {
        destination[destinationIndex] = source[sourceIndex];
    }
}
