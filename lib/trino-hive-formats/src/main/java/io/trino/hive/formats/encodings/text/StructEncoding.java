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
package io.trino.hive.formats.encodings.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.FileCorruptionException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;

import java.util.List;
import java.util.stream.IntStream;

public class StructEncoding
        extends BlockEncoding
{
    private final RowType rowType;
    private final byte separator;
    private final boolean lastColumnTakesRest;
    private final List<TextColumnEncoding> structFields;
    private final List<Integer> fieldOffsets;

    public StructEncoding(
            RowType rowType,
            Slice nullSequence,
            byte separator,
            Byte escapeByte,
            boolean lastColumnTakesRest,
            List<TextColumnEncoding> structFields)
    {
        super(rowType, nullSequence, escapeByte);
        this.rowType = rowType;
        this.separator = separator;
        this.lastColumnTakesRest = lastColumnTakesRest;
        this.structFields = structFields;
        this.fieldOffsets = IntStream.range(0, structFields.size())
                .boxed()
                .toList();
    }

    public StructEncoding(
            RowType rowType,
            Slice nullSequence,
            byte separator,
            Byte escapeByte,
            boolean lastColumnTakesRest,
            List<TextColumnEncoding> structFields,
            List<Integer> fieldOffsets)
    {
        super(rowType, nullSequence, escapeByte);
        this.rowType = rowType;
        this.separator = separator;
        this.lastColumnTakesRest = lastColumnTakesRest;
        this.structFields = structFields;
        this.fieldOffsets = fieldOffsets;
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
            throws FileCorruptionException
    {
        SqlRow row = rowType.getObject(block, position);
        int rawIndex = row.getRawIndex();
        for (int fieldIndex = 0; fieldIndex < structFields.size(); fieldIndex++) {
            if (fieldIndex > 0) {
                output.writeByte(separator);
            }

            Block fieldBlock = row.getRawFieldBlock(fieldIndex);
            if (fieldBlock.isNull(rawIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                structFields.get(fieldIndex).encodeValueInto(fieldBlock, rawIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        int end = offset + length;
        ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
            int currentOffset = offset;
            int elementOffset = currentOffset;
            int fieldIndex = 0;
            while (currentOffset < end) {
                byte currentByte = slice.getByte(currentOffset);
                if (currentByte == separator) {
                    Integer fieldOffset = fieldOffsets.get(fieldIndex);
                    if (fieldOffset != null) {
                        decodeElementValueInto(fieldIndex, fieldBuilders.get(fieldOffset), slice, elementOffset, currentOffset - elementOffset);
                    }
                    elementOffset = currentOffset + 1;
                    fieldIndex++;
                    if (lastColumnTakesRest && fieldIndex == structFields.size() - 1) {
                        // no need to process the remaining bytes as they are all assigned to the last column
                        break;
                    }
                    if (fieldIndex == structFields.size()) {
                        // this was the last field, so there is no more data to process
                        return;
                    }
                }
                else if (isEscapeByte(currentByte)) {
                    // ignore the char after escape_char
                    currentOffset++;
                }
                currentOffset++;
            }
            Integer fieldOffset = fieldOffsets.get(fieldIndex);
            if (fieldOffset != null) {
                decodeElementValueInto(fieldIndex, fieldBuilders.get(fieldOffset), slice, elementOffset, end - elementOffset);
            }
            fieldIndex++;

            // missing fields are null
            while (fieldIndex < structFields.size()) {
                fieldBuilders.get(fieldIndex).appendNull();
                fieldIndex++;
            }
        });
    }

    private void decodeElementValueInto(int fieldIndex, BlockBuilder builder, Slice slice, int offset, int length)
            throws FileCorruptionException
    {
        // ignore extra fields
        if (fieldIndex >= structFields.size()) {
            return;
        }

        if (isNullSequence(slice, offset, length)) {
            builder.appendNull();
        }
        else {
            structFields.get(fieldIndex).decodeValueInto(builder, slice, offset, length);
        }
    }
}
