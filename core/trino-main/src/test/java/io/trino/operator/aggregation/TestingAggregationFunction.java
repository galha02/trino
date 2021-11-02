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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.BoundSignature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;

public class TestingAggregationFunction
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private final InternalAggregationFunction function;
    private final List<Type> parameterTypes;
    private final Type finalType;

    public TestingAggregationFunction(BoundSignature signature, AggregationMetadata aggregationMetadata)
    {
        this.parameterTypes = signature.getArgumentTypes();
        this.finalType = signature.getReturnType();
        this.function = new InternalAggregationFunction(signature, aggregationMetadata);
    }

    public int getParameterCount()
    {
        return parameterTypes.size();
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return function.bind(inputChannels, maskChannel);
    }

    public AccumulatorFactory bindDistinct(
            List<Integer> inputChannels,
            List<Type> sourceTypes,
            Optional<Integer> maskChannel)
    {
        return function.bind(
                inputChannels,
                maskChannel,
                sourceTypes,
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                true, // distinct
                new JoinCompiler(TYPE_OPERATORS),
                new BlockTypeOperators(TYPE_OPERATORS),
                ImmutableList.of(),
                TEST_SESSION);
    }
}
