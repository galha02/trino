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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;

public class TestShortDecimalSumAggregation
        extends AbstractTestDecimalSumAggregation
{
    DecimalType shortDecimalType = createDecimalType(MAX_SHORT_PRECISION - 1);

    @Override
    protected DecimalType getDecimalType()
    {
        return shortDecimalType;
    }

    @Override
    protected void writeDecimalToBlock(BigDecimal decimal, BlockBuilder blockBuilder)
    {
        shortDecimalType.writeLong(blockBuilder, decimal.unscaledValue().longValue());
    }

    @Override
    protected List<Type> getFunctionParameterTypes()
    {
        return ImmutableList.of(createDecimalType(16, 2));
    }
}
