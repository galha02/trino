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
package io.trino.operator.aggregation;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

public class MapAggregationStateSerializer
        implements AccumulatorStateSerializer<MapAggregationState>
{
    private final Type serializedType;

    public MapAggregationStateSerializer(@TypeParameter("map(K, V)") Type serializedType)
    {
        this.serializedType = serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MapAggregationState state, BlockBuilder out)
    {
        state.writeAll((MapBlockBuilder) out);
    }

    @Override
    public void deserialize(Block block, int index, MapAggregationState state)
    {
        SqlMap sqlMap = (SqlMap) serializedType.getObject(block, index);
        ((SingleMapAggregationState) state).setTempSerializedState(sqlMap);
    }
}
