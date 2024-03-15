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
package io.trino.plugin.deltalake.kernel.data;

import io.delta.kernel.types.DataType;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;

import static java.util.Objects.requireNonNull;

public class TrinoColumnVectorWrapper
        extends AbstractTrinoColumnVectorWrapper
{
    private final DataType deltaType;
    private final Block trinoBlock;

    public TrinoColumnVectorWrapper(DataType deltaType, Block trinoBlock)
    {
        this.trinoBlock = requireNonNull(trinoBlock, "trinoBlock is null");
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoBlock;
    }

    @Override
    public int getSize()
    {
        return trinoBlock.getPositionCount();
    }

    @Override
    public DataType getDataType()
    {
        return deltaType;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return trinoBlock.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return ((ByteArrayBlock) trinoBlock).getByte(rowId) != 0;
    }

    @Override
    public byte getByte(int rowId)
    {
        return ((ByteArrayBlock) trinoBlock).getByte(rowId);
    }

    @Override
    public short getShort(int rowId)
    {
        return (short) ((IntArrayBlock) trinoBlock).getInt(rowId);
    }

    @Override
    public long getLong(int rowId)
    {
        return ((LongArrayBlock) trinoBlock).getLong(rowId);
    }

    @Override
    public int getInt(int rowId)
    {
        return ((IntArrayBlock) trinoBlock).getInt(rowId);
    }
}
