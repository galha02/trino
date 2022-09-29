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
package io.trino.spi;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.LazyBlock;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.DictionaryBlock.createProjectedDictionaryBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Page
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(Page.class).instanceSize();
    private static final Block[] EMPTY_BLOCKS = new Block[0];

    /**
     * Visible to give trusted classes like {@link PageBuilder} access to a constructor that doesn't
     * defensively copy the blocks
     */
    static Page wrapBlocksWithoutCopy(int positionCount, Block[] blocks)
    {
        return new Page(false, positionCount, blocks);
    }

    private final Block[] blocks;
    private final int positionCount;
    private volatile long sizeInBytes = -1;
    private volatile long retainedSizeInBytes = -1;
    private volatile long logicalSizeInBytes = -1;

    public Page(Block... blocks)
    {
        this(true, determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount)
    {
        this(false, positionCount, EMPTY_BLOCKS);
    }

    public Page(int positionCount, Block... blocks)
    {
        this(true, positionCount, blocks);
    }

    private Page(boolean blocksCopyRequired, int positionCount, Block[] blocks)
    {
        requireNonNull(blocks, "blocks is null");
        this.positionCount = positionCount;
        if (blocks.length == 0) {
            this.blocks = EMPTY_BLOCKS;
            this.sizeInBytes = 0;
            this.logicalSizeInBytes = 0;
            // Empty blocks are not considered "retained" by any particular page
            this.retainedSizeInBytes = INSTANCE_SIZE;
        }
        else {
            this.blocks = blocksCopyRequired ? blocks.clone() : blocks;
        }
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes;
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (Block block : blocks) {
                sizeInBytes += block.getLoadedBlock().getSizeInBytes();
            }
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    public long getLogicalSizeInBytes()
    {
        long logicalSizeInBytes = this.logicalSizeInBytes;
        if (logicalSizeInBytes < 0) {
            logicalSizeInBytes = 0;
            for (Block block : blocks) {
                logicalSizeInBytes += block.getLogicalSizeInBytes();
            }
            this.logicalSizeInBytes = logicalSizeInBytes;
        }
        return logicalSizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        if (retainedSizeInBytes < 0) {
            return updateRetainedSize();
        }
        return retainedSizeInBytes;
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    /**
     * Gets the values at the specified position as a single element page.  The method creates independent
     * copy of the data.
     */
    public Page getSingleValuePage(int position)
    {
        Block[] singleValueBlocks = new Block[this.blocks.length];
        for (int i = 0; i < this.blocks.length; i++) {
            singleValueBlocks[i] = this.blocks[i].getSingleValueBlock(position);
        }
        return wrapBlocksWithoutCopy(1, singleValueBlocks);
    }

    public Page getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in page with %s positions", positionOffset, length, positionCount));
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return wrapBlocksWithoutCopy(length, slicedBlocks);
    }

    public Page appendColumn(Block block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        newBlocks[blocks.length] = block;
        return wrapBlocksWithoutCopy(positionCount, newBlocks);
    }

    public void compact()
    {
        if (getRetainedSizeInBytes() <= getSizeInBytes()) {
            return;
        }

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                continue;
            }
            // Compact the block
            blocks[i] = block.copyRegion(0, block.getPositionCount());
        }

        Map<DictionaryId, DictionaryBlockIndexes> dictionaryBlocks = getRelatedDictionaryBlocks();
        for (DictionaryBlockIndexes blockIndexes : dictionaryBlocks.values()) {
            List<DictionaryBlock> compactBlocks = DictionaryBlock.compactRelatedBlocks(blockIndexes.getBlocks());
            List<Integer> indexes = blockIndexes.getIndexes();
            for (int i = 0; i < compactBlocks.size(); i++) {
                blocks[indexes.get(i)] = compactBlocks.get(i);
            }
        }

        updateRetainedSize();
    }

    private Map<DictionaryId, DictionaryBlockIndexes> getRelatedDictionaryBlocks()
    {
        Map<DictionaryId, DictionaryBlockIndexes> relatedDictionaryBlocks = new HashMap<>();

        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), id -> new DictionaryBlockIndexes())
                        .addBlock(dictionaryBlock, i);
            }
        }
        return relatedDictionaryBlocks;
    }

    /**
     * Returns a page that assures all data is in memory.
     * May return the same page if all page data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    public Page getLoadedPage()
    {
        for (int i = 0; i < blocks.length; i++) {
            Block loaded = blocks[i].getLoadedBlock();
            if (loaded != blocks[i]) {
                // Transition to new block creation mode after the first newly loaded block is encountered
                Block[] loadedBlocks = blocks.clone();
                loadedBlocks[i++] = loaded;
                for (; i < blocks.length; i++) {
                    loadedBlocks[i] = blocks[i].getLoadedBlock();
                }
                return wrapBlocksWithoutCopy(positionCount, loadedBlocks);
            }
        }
        // No newly loaded blocks
        return this;
    }

    public Page getLoadedPage(int column)
    {
        return wrapBlocksWithoutCopy(positionCount, new Block[] {this.blocks[column].getLoadedBlock()});
    }

    public Page getLoadedPage(int... columns)
    {
        requireNonNull(columns, "columns is null");

        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blocks[i] = this.blocks[columns[i]].getLoadedBlock();
        }
        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    public Page getLoadedPage(int[] columns, int[] eagerlyLoadedColumns)
    {
        requireNonNull(columns, "columns is null");

        for (int column : eagerlyLoadedColumns) {
            this.blocks[column] = this.blocks[column].getLoadedBlock();
        }
        if (retainedSizeInBytes != -1 && eagerlyLoadedColumns.length > 0) {
            updateRetainedSize();
        }
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blocks[i] = this.blocks[columns[i]];
        }

        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    public Page getPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        if (offset != 0) {
            retainedPositions = Arrays.copyOfRange(retainedPositions, offset, offset + length);
        }
        MultiBlockPositionsExtractor multiBlockPositionsExtractor = new MultiBlockPositionsExtractor(retainedPositions, length);
        Block[] blocks = new Block[this.blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = multiBlockPositionsExtractor.getPositions(this.blocks[i]);
        }
        return wrapBlocksWithoutCopy(length, blocks);
    }

    public Page copyPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        Block[] blocks = new Block[this.blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = this.blocks[i].copyPositions(retainedPositions, offset, length);
        }
        return wrapBlocksWithoutCopy(length, blocks);
    }

    public Page getColumns(int column)
    {
        return wrapBlocksWithoutCopy(positionCount, new Block[] {this.blocks[column]});
    }

    public Page getColumns(int... columns)
    {
        requireNonNull(columns, "columns is null");

        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blocks[i] = this.blocks[columns[i]];
        }
        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    public Page prependColumn(Block column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }

        Block[] result = new Block[blocks.length + 1];
        result[0] = column;
        System.arraycopy(blocks, 0, result, 1, blocks.length);

        return wrapBlocksWithoutCopy(positionCount, result);
    }

    private long updateRetainedSize()
    {
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(blocks);
        for (Block block : blocks) {
            retainedSizeInBytes += block.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes = retainedSizeInBytes;
        return retainedSizeInBytes;
    }

    private static class DictionaryBlockIndexes
    {
        private final List<DictionaryBlock> blocks = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();

        public void addBlock(DictionaryBlock block, int index)
        {
            blocks.add(block);
            indexes.add(index);
        }

        public List<DictionaryBlock> getBlocks()
        {
            return blocks;
        }

        public List<Integer> getIndexes()
        {
            return indexes;
        }
    }

    private static class MultiBlockPositionsExtractor
    {
        private final Map<DictionaryId, DictionaryIds> unnestedIds = new HashMap<>();
        private final DictionaryId newDictionaryId = randomDictionaryId();
        private final int[] positions;
        private final int length;

        private MultiBlockPositionsExtractor(int[] positions, int length)
        {
            this.positions = positions;
            this.length = length;
        }

        /**
         * Returns new Block with only selected {@link #positions}.
         * For results that are {@link DictionaryBlock}s, if the {@link DictionaryBlock#ids} are equal
         * then also {@link DictionaryBlock#getDictionarySourceId()} will be equal.
         */
        public Block getPositions(Block block)
        {
            if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                DictionaryIds ids = unnestedIds.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), key -> unnestDictionaryIds(dictionaryBlock, positions));
                return createProjectedDictionaryBlock(ids.ids.length, dictionaryBlock.getDictionary(), ids.ids, ids.dictionaryId);
            }

            if (block instanceof LazyBlock) {
                // because putting dictionary block over lazy block might produce nested dictionary blocks
                return block.getPositions(positions, 0, length);
            }

            // use dictionary to project selected positions in order to avoid data copy
            return createProjectedDictionaryBlock(length, block, positions, newDictionaryId);
        }

        // based on io.trino.spi.block.DictionaryBlock.getPositions
        private static DictionaryIds unnestDictionaryIds(DictionaryBlock block, int[] positions)
        {
            int length = positions.length;
            Block dictionary = block.getDictionary();
            int[] newIds = new int[length];
            boolean isCompact = block.isCompact() && length >= dictionary.getPositionCount();
            boolean[] seen = null;
            if (isCompact) {
                seen = new boolean[dictionary.getPositionCount()];
            }
            for (int i = 0; i < length; i++) {
                newIds[i] = block.getId(positions[i]);
                if (isCompact) {
                    seen[newIds[i]] = true;
                }
            }
            for (int i = 0; i < dictionary.getPositionCount() && isCompact; i++) {
                isCompact = seen[i];
            }
            return new DictionaryIds(newIds);
        }
    }

    private static class DictionaryIds
    {
        private final DictionaryId dictionaryId = DictionaryId.randomDictionaryId();
        private final int[] ids;

        private DictionaryIds(int[] ids)
        {
            this.ids = requireNonNull(ids, "ids is null");
        }
    }
}
