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
package io.prestosql.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;

public class LongArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "LONG_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        if (!block.mayHaveNull()) {
            sliceOutput.writeBytes(getValuesSlice(block));
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                if (!block.isNull(position)) {
                    sliceOutput.writeLong(block.getLong(position, 0));
                }
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount];
        if (valueIsNull == null) {
            sliceInput.readBytes(Slices.wrappedLongArray(values));
        }
        else {
            for (int position = 0; position < values.length; position++) {
                if (!valueIsNull[position]) {
                    values[position] = sliceInput.readLong();
                }
            }
        }

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }

    private Slice getValuesSlice(Block block)
    {
        if (block instanceof LongArrayBlock) {
            return ((LongArrayBlock) block).getValuesSlice();
        }
        else if (block instanceof LongArrayBlockBuilder) {
            return ((LongArrayBlockBuilder) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}
