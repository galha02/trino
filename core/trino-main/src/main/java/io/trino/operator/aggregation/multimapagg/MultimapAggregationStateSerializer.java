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
package io.trino.operator.aggregation.multimapagg;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

public class MultimapAggregationStateSerializer
        implements AccumulatorStateSerializer<MultimapAggregationState>
{
    private final MapType serializedType;

    public MultimapAggregationStateSerializer(@TypeParameter("map(K, Array(V))") Type serializedType)
    {
        this.serializedType = (MapType) serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MultimapAggregationState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }

    @Override
    public void deserialize(Block block, int index, MultimapAggregationState state)
    {
        SqlMap sqlMap = serializedType.getObject(block, index);
        ((SingleMultimapAggregationState) state).setTempSerializedState(sqlMap);
    }
}
