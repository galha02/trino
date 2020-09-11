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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import java.util.List;

import static io.prestosql.spi.type.DoubleType.DOUBLE;

public class TestDoubleVariancePopAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    protected Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, (double) i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }

        Variance variance = new Variance(false);
        return variance.evaluate(values);
    }

    @Override
    protected String getFunctionName()
    {
        return "var_pop";
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(DOUBLE);
    }
}
