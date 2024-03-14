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
package io.trino.operator.aggregation.state;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongDecimalWithOverflowAndLongStateSerializer
{
    private static final LongDecimalWithOverflowAndLongStateFactory STATE_FACTORY = new LongDecimalWithOverflowAndLongStateFactory();

    @Test
    public void testSerde()
    {
        testSerde(3, 0, 0, 1, 1);
        testSerde(3, 5, 0, 1, 2);
        testSerde(3, 5, 7, 1, 4);
        testSerde(3, 0, 0, 2, 3);
        testSerde(3, 5, 0, 2, 4);
        testSerde(3, 5, 7, 2, 4);
        testSerde(3, 0, 7, 1, 3);
        testSerde(3, 0, 7, 2, 3);
        testSerde(0, 0, 0, 1, 1);
        testSerde(0, 5, 0, 1, 2);
        testSerde(0, 5, 7, 1, 4);
        testSerde(0, 0, 0, 2, 3);
        testSerde(0, 5, 0, 2, 4);
        testSerde(0, 5, 7, 2, 4);
        testSerde(0, 0, 7, 1, 3);
        testSerde(0, 0, 7, 2, 3);
    }

    private void testSerde(long low, long high, long overflow, long count, int expectedLength)
    {
        testSerde(low, high, overflow, count, expectedLength, Function.identity());
    }

    private void testSerde(long low, long high, long overflow, long count, int expectedLength, Function<Block, Block> serializedModification)
    {
        LongDecimalWithOverflowAndLongState state = STATE_FACTORY.createSingleState();
        state.getDecimalArray()[0] = high;
        state.getDecimalArray()[1] = low;
        state.setOverflow(overflow);
        state.setLong(count);

        LongDecimalWithOverflowAndLongState outState = roundTrip(state, expectedLength, serializedModification);

        assertThat(outState.getDecimalArray()[0]).isEqualTo(high);
        assertThat(outState.getDecimalArray()[1]).isEqualTo(low);
        assertThat(outState.getOverflow()).isEqualTo(overflow);
        assertThat(outState.getLong()).isEqualTo(count);
    }

    @Test
    public void testNullSerde()
    {
        // state is created null
        LongDecimalWithOverflowAndLongState state = STATE_FACTORY.createSingleState();

        LongDecimalWithOverflowAndLongState outState = roundTrip(state, 0);

        assertThat(outState.getLong()).isEqualTo(0);
    }

    @Test
    public void testDictionaryDeserialization()
    {
        testSerde(3, 0, 0, 1, 1, block -> DictionaryBlock.create(2, block, new int[] {0, 0}));
    }

    @Test
    public void testRleDeserialization()
    {
        testSerde(3, 0, 0, 1, 1, block -> RunLengthEncodedBlock.create(block, 2));
    }

    private LongDecimalWithOverflowAndLongState roundTrip(LongDecimalWithOverflowAndLongState state, int expectedLength)
    {
        return roundTrip(state, expectedLength, Function.identity());
    }

    private LongDecimalWithOverflowAndLongState roundTrip(LongDecimalWithOverflowAndLongState state, int expectedLength, Function<Block, Block> serializedModification)
    {
        LongDecimalWithOverflowAndLongStateSerializer serializer = new LongDecimalWithOverflowAndLongStateSerializer();
        VariableWidthBlockBuilder out = new VariableWidthBlockBuilder(null, 1, 0);

        serializer.serialize(state, out);

        VariableWidthBlock serialized = out.buildValueBlock();
        assertThat(serialized.getSliceLength(0)).isEqualTo(expectedLength * Long.BYTES);
        LongDecimalWithOverflowAndLongState outState = STATE_FACTORY.createSingleState();
        serializer.deserialize(serializedModification.apply(serialized), 0, outState);
        return outState;
    }
}
