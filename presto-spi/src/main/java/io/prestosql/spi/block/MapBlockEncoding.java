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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSerde;

import java.util.Optional;

import static io.airlift.slice.Slices.wrappedIntArray;
import static io.prestosql.spi.block.MapBlock.createMapBlockInternal;
import static io.prestosql.spi.block.MapHashTables.HASH_MULTIPLIER;
import static java.lang.String.format;

public class MapBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "MAP";

    private final TypeManager typeManager;

    public MapBlockEncoding(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractMapBlock mapBlock = (AbstractMapBlock) block;

        int positionCount = mapBlock.getPositionCount();

        int offsetBase = mapBlock.getOffsetBase();
        int[] offsets = mapBlock.getOffsets();
        Optional<int[]> hashTable = mapBlock.getHashTables().tryGet();

        int entriesStartOffset = offsets[offsetBase];
        int entriesEndOffset = offsets[offsetBase + positionCount];

        TypeSerde.writeType(sliceOutput, mapBlock.getMapType());

        blockEncodingSerde.writeBlock(sliceOutput, mapBlock.getRawKeyBlock().getRegion(entriesStartOffset, entriesEndOffset - entriesStartOffset));
        blockEncodingSerde.writeBlock(sliceOutput, mapBlock.getRawValueBlock().getRegion(entriesStartOffset, entriesEndOffset - entriesStartOffset));

        if (hashTable.isPresent()) {
            int hashTableLength = (entriesEndOffset - entriesStartOffset) * HASH_MULTIPLIER;
            sliceOutput.appendInt(hashTableLength); // hashtable length
            sliceOutput.writeBytes(wrappedIntArray(hashTable.get(), entriesStartOffset * HASH_MULTIPLIER, hashTableLength));
        }
        else {
            // if the hashTable is null, we write the length -1
            sliceOutput.appendInt(-1);  // hashtable length
        }

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(offsets[offsetBase + position] - entriesStartOffset);
        }
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        MapType mapType = (MapType) TypeSerde.readType(typeManager, sliceInput);

        Block keyBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valueBlock = blockEncodingSerde.readBlock(sliceInput);

        int hashTableLength = sliceInput.readInt();
        int[] hashTable = null;
        if (hashTableLength >= 0) {
            hashTable = new int[hashTableLength];
            sliceInput.readBytes(wrappedIntArray(hashTable));
        }

        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format(
                    "Deserialized MapBlock violates invariants: key %s, value %s",
                    keyBlock.getPositionCount(),
                    valueBlock.getPositionCount()));
        }

        if (hashTable != null && keyBlock.getPositionCount() * HASH_MULTIPLIER != hashTable.length) {
            throw new IllegalArgumentException(format(
                    "Deserialized MapBlock violates invariants: expected hashtable size %s, actual hashtable size %s",
                    keyBlock.getPositionCount() * HASH_MULTIPLIER,
                    hashTable.length));
        }

        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(wrappedIntArray(offsets));
        Optional<boolean[]> mapIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        MapHashTables hashTables = new MapHashTables(mapType, Optional.ofNullable(hashTable));
        return createMapBlockInternal(mapType, 0, positionCount, mapIsNull, offsets, keyBlock, valueBlock, hashTables);
    }
}
