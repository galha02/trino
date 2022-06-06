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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetSchemaConverter
{
    @Test
    public void testDecimalTypeLength()
    {
        for (int precision = 1; precision <= MAX_PRECISION; precision++) {
            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    ImmutableList.of(createDecimalType(precision)),
                    ImmutableList.of("test"));
            PrimitiveType primitiveType = schemaConverter.getMessageType().getType(0).asPrimitiveType();
            if (precision <= 9) {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
            }
            else if (precision <= MAX_SHORT_PRECISION) {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
            }
            else {
                assertThat(primitiveType.getPrimitiveTypeName())
                        .isEqualTo(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
                assertThat(primitiveType.getTypeLength()).isBetween(9, 16);
                BigInteger bigInteger = new BigInteger("9".repeat(precision));
                assertThat(bigInteger.toByteArray().length).isEqualTo(primitiveType.getTypeLength());
            }
        }
    }
}
