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
package io.trino.operator.aggregation.minmaxn;

import io.trino.operator.aggregation.minmaxn.MinMaxNStateFactory.SingleMinMaxNState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public abstract class MinMaxNStateSerializer<T extends MinMaxNState>
        implements AccumulatorStateSerializer<T>
{
    private final Type serializedType;

    public MinMaxNStateSerializer(Type serializedType)
    {
        this.serializedType = requireNonNull(serializedType, "serializedType is null");
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(T state, BlockBuilder out)
    {
        state.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, T state)
    {
        // the aggregation framework uses a scratch single state for deserialization, and then calls the combine function
        // for typed heap is is simpler to store the deserialized row block in the state and then add the row block
        // directly to the heap in the combine
        SqlRow sqlRow = (SqlRow) serializedType.getObject(block, index);
        ((SingleMinMaxNState) state).setTempSerializedState(sqlRow);
    }
}
