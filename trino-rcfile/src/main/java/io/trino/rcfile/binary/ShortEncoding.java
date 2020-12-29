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
package io.trino.rcfile.binary;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.rcfile.ColumnData;
import io.trino.rcfile.EncodeOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

public class ShortEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public ShortEncoding(Type type)
    {
        this.type = type;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                output.writeShort(Short.reverseBytes((short) type.getLong(block, position)));
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        output.writeShort(Short.reverseBytes((short) type.getLong(block, position)));
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            if (length != 0) {
                checkState(length == SIZE_OF_SHORT, "Short should be 2 bytes");
                type.writeLong(builder, (long) Short.reverseBytes(slice.getShort(columnData.getOffset(i))));
            }
            else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    @Override
    public int getValueOffset(Slice slice, int offset)
    {
        return 0;
    }

    @Override
    public int getValueLength(Slice slice, int offset)
    {
        return SIZE_OF_SHORT;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeLong(builder, Short.reverseBytes(slice.getShort(offset)));
    }
}
