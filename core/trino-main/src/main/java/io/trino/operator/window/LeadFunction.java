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
package io.trino.operator.window;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ValueWindowFunction;
import io.trino.spi.function.WindowFunctionSignature;

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

@WindowFunctionSignature(name = "lead", typeVariable = "T", returnType = "T", argumentTypes = "T")
@WindowFunctionSignature(name = "lead", typeVariable = "T", returnType = "T", argumentTypes = {"T", "bigint"})
@WindowFunctionSignature(name = "lead", typeVariable = "T", returnType = "T", argumentTypes = {"T", "bigint", "T"})
public class LeadFunction
        extends ValueWindowFunction
{
    private final int valueChannel;
    private final int offsetChannel;
    private final int defaultChannel;
    private final boolean ignoreNulls;

    public LeadFunction(List<Integer> argumentChannels, boolean ignoreNulls)
    {
        this.valueChannel = argumentChannels.get(0);
        this.offsetChannel = (argumentChannels.size() > 1) ? argumentChannels.get(1) : -1;
        this.defaultChannel = (argumentChannels.size() > 2) ? argumentChannels.get(2) : -1;
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        checkCondition(offsetChannel < 0 || !windowIndex.isNull(offsetChannel, currentPosition), INVALID_FUNCTION_ARGUMENT, "Offset must not be null");

        long offset = (offsetChannel < 0) ? 1 : windowIndex.getLong(offsetChannel, currentPosition);
        checkCondition(offset >= 0, INVALID_FUNCTION_ARGUMENT, "Offset must be at least 0");

        long valuePosition;

        if (ignoreNulls && (offset > 0)) {
            long count = 0;
            valuePosition = currentPosition + 1;
            while (withinPartition(valuePosition)) {
                if (!windowIndex.isNull(valueChannel, toIntExact(valuePosition))) {
                    count++;
                    if (count == offset) {
                        break;
                    }
                }
                valuePosition++;
            }
        }
        else {
            valuePosition = currentPosition + offset;
        }

        if (withinPartition(valuePosition)) {
            windowIndex.appendTo(valueChannel, toIntExact(valuePosition), output);
        }
        else if (defaultChannel >= 0) {
            windowIndex.appendTo(defaultChannel, currentPosition, output);
        }
        else {
            output.appendNull();
        }
    }

    private boolean withinPartition(long valuePosition)
    {
        return valuePosition >= 0 && valuePosition < windowIndex.size();
    }
}
