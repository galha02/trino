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

import java.util.List;

public interface ColumnAdapter<BufferType>
{
    /**
     * Temporary buffer used for null unpacking
     */
    default BufferType createTemporaryBuffer(int currentOffset, int size, BufferType buffer)
    {
        return createBuffer(size);
    }

    BufferType createBuffer(int size);

    void copyValue(BufferType source, int sourceIndex, BufferType destination, int destinationIndex);

    Block createNullableBlock(byte[] nulls, BufferType values);

    default Block createNullableDictionaryBlock(BufferType dictionary, int nonNullsCount)
    {
        byte[] nulls = new byte[nonNullsCount + 1];
        nulls[nonNullsCount] = 1;
        return createNullableBlock(nulls, dictionary);
    }

    Block createNonNullBlock(BufferType values);

    default void unpackNullValues(BufferType source, BufferType destination, byte[] isNull, int destOffset, int nonNullCount, int totalValuesCount)
    {
        int srcOffset = 0;
        while (srcOffset < nonNullCount) {
            copyValue(source, srcOffset, destination, destOffset);
            // Avoid branching
            srcOffset += (byte) (isNull[destOffset] != 0 ? 0 : 1);
            destOffset++;
        }
    }

    void decodeDictionaryIds(BufferType values, int offset, int length, int[] ids, BufferType dictionary);

    long getSizeInBytes(BufferType values);

    BufferType merge(List<BufferType> buffers);
}
