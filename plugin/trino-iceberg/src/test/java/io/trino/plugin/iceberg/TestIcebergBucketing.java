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
package io.trino.plugin.iceberg;

import com.google.common.primitives.Primitives;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.UUIDType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.iceberg.PartitionTransforms.getBucketTransform;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.apache.iceberg.types.Type.TypeID.DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestIcebergBucketing
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testBucketNumberCompare()
    {
        // TODO cover other types like timestamp, date or time

        // TODO make sure all test cases from https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements are included here

        assertBucketAndHashEquals("int", null, null);
        assertBucketAndHashEquals("int", 0, 1669671676);
        assertBucketAndHashEquals("int", 300_000, 1798339266);
        assertBucketAndHashEquals("int", Integer.MIN_VALUE, 74448856);
        assertBucketAndHashEquals("int", Integer.MAX_VALUE, 1819228606);

        assertBucketAndHashEquals("long", null, null);
        assertBucketAndHashEquals("long", 0L, 1669671676);
        assertBucketAndHashEquals("long", 300_000_000_000L, 371234369);
        assertBucketAndHashEquals("long", Long.MIN_VALUE, 1366273829);
        assertBucketAndHashEquals("long", Long.MAX_VALUE, 40977599);

        assertBucketAndHashEquals("decimal(1, 1)", null, null);
        assertBucketAndHashEquals("decimal(1, 0)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(1, 0)", "1", 1683673515);
        assertBucketAndHashEquals("decimal(1, 0)", "9", 1771774483);
        assertBucketAndHashEquals("decimal(1, 0)", "-9", 156162024);
        assertBucketAndHashEquals("decimal(3, 1)", "0.1", 1683673515);
        assertBucketAndHashEquals("decimal(3, 1)", "1.0", 307159211);
        assertBucketAndHashEquals("decimal(3, 1)", "12.3", 1308316337);
        assertBucketAndHashEquals("decimal(3, 1)", "-12.3", 1847027525);
        assertBucketAndHashEquals("decimal(18, 10)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(38, 10)", null, null);
        assertBucketAndHashEquals("decimal(38, 10)", "999999.9999999999", 1053577599);
        assertBucketAndHashEquals("decimal(38, 10)", "-999999.9999999999", 1054888790);
        assertBucketAndHashEquals("decimal(38, 0)", "99999999999999999999999999999999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 0)", "-99999999999999999999999999999999999999", 193266010);
        assertBucketAndHashEquals("decimal(38, 10)", "9999999999999999999999999999.9999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 10)", "-9999999999999999999999999999.9999999999", 193266010);
        assertBucketAndHashEquals("decimal(38, 10)", "123456789012345.0", 93815101);
        assertBucketAndHashEquals("decimal(38, 10)", "-123456789012345.0", 522439017);

        assertBucketAndHashEquals("string", null, null);
        assertBucketAndHashEquals("string", "", 0);
        assertBucketAndHashEquals("string", "test string", 671244848);
        assertBucketAndHashEquals("string", "Trino rocks", 2131833594);
        assertBucketAndHashEquals("string", "\u5f3a\u5927\u7684Trino\u5f15\u64ce", 822296301); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        // TODO https://github.com/apache/iceberg/issues/2837: Iceberg value is incorrect
        assertThatThrownBy(() -> assertBucketAndHashEquals("string", "\uD83D\uDCB0", 661122892)) // 4-byte UTF-8 codepoint (non-BMP)
                .isInstanceOf(AssertionError.class)
                .hasMessage("icebergType=string, bucketCount=2147483647, icebergBucket=468848164, trinoBucket=661122892; expected [468848164] but found [661122892]");
        // TODO https://github.com/apache/iceberg/issues/2837: Iceberg value is incorrect
        assertThatThrownBy(() -> assertBucketAndHashEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", 2094039023)) // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2
                .isInstanceOf(AssertionError.class)
                .hasMessage("icebergType=string, bucketCount=2147483647, icebergBucket=775615312, trinoBucket=2094039023; expected [775615312] but found [2094039023]");

        assertBucketAndHashEquals("binary", null, null);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap(new byte[] {}), 0);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("hello trino".getBytes(StandardCharsets.UTF_8)), 493441885);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF".getBytes(StandardCharsets.UTF_16)), 1291558121);
    }

    @Test(dataProvider = "unsupportedBucketingTypes")
    public void testUnsupportedTypes(Type type)
    {
        assertThatThrownBy(() -> computeIcebergBucket(type, null, 1))
                .hasMessage(format("Cannot bucket by type: %s", type));

        assertThatThrownBy(() -> computeTrinoBucket(type, null, 1))
                .hasMessage(format("Unsupported type for 'bucket': %s", toTrinoType(type, TYPE_MANAGER)));
    }

    @DataProvider
    public Object[][] unsupportedBucketingTypes()
    {
        return new Object[][] {
                {BooleanType.get()},
                {FloatType.get()},
                {DoubleType.get()},
        };
    }

    @Test(dataProvider = "unsupportedTrinoBucketingTypes")
    public void testUnsupportedTrinoTypes(Type type)
    {
        assertThatThrownBy(() -> computeTrinoBucket(type, null, 1))
                .hasMessage(format("Cannot convert from Iceberg type '%s' (%s) to Trino type", type, type.typeId()));

        assertNull(computeIcebergBucket(type, null, 1));
    }

    @DataProvider
    public Object[][] unsupportedTrinoBucketingTypes()
    {
        // TODO https://github.com/trinodb/trino/issues/6663 Iceberg UUID should be mapped to Trino UUID and supported for bucketing
        return new Object[][] {
                {UUIDType.get()},
        };
    }

    private void assertBucketAndHashEquals(String icebergTypeName, Object icebergValue, Integer expectedHash)
    {
        Type icebergType = Types.fromPrimitiveString(icebergTypeName);
        if (icebergValue != null && icebergType.typeId() == DECIMAL) {
            icebergValue = new BigDecimal((String) icebergValue).setScale(((DecimalType) icebergType).scale());
        }

        assertBucketEquals(icebergType, icebergValue);
        assertHashEquals(icebergType, icebergValue, expectedHash);
    }

    private void assertBucketEquals(Type icebergType, Object icebergValue)
    {
        assertBucketNumberEquals(icebergType, icebergValue, Integer.MAX_VALUE);

        assertBucketNumberEquals(icebergType, icebergValue, 2);
        assertBucketNumberEquals(icebergType, icebergValue, 7);
        assertBucketNumberEquals(icebergType, icebergValue, 31);
        assertBucketNumberEquals(icebergType, icebergValue, 32);
        assertBucketNumberEquals(icebergType, icebergValue, 100);
        assertBucketNumberEquals(icebergType, icebergValue, 10000);
        assertBucketNumberEquals(icebergType, icebergValue, 524287); // prime number
        assertBucketNumberEquals(icebergType, icebergValue, 1 << 30);
    }

    private void assertBucketNumberEquals(Type icebergType, Object icebergValue, int bucketCount)
    {
        Integer icebergBucket = computeIcebergBucket(icebergType, icebergValue, bucketCount);
        Integer trinoBucket = computeTrinoBucket(icebergType, icebergValue, bucketCount);

        assertEquals(
                trinoBucket,
                icebergBucket,
                format("icebergType=%s, bucketCount=%s, icebergBucket=%d, trinoBucket=%d;", icebergType, bucketCount, icebergBucket, trinoBucket));
    }

    private void assertHashEquals(Type icebergType, Object icebergValue, Integer expectedHash)
    {
        // In Iceberg, hash is 31-bit number (no sign), so computing bucket number for Integer.MAX_VALUE gives as back actual
        // hash value (except when hash equals Integer.MAX_VALUE).

        Integer icebergBucketHash = computeIcebergBucket(icebergType, icebergValue, Integer.MAX_VALUE);
        Integer trinoBucketHash = computeTrinoBucket(icebergType, icebergValue, Integer.MAX_VALUE);

        // Ensure hash is stable and does not change
        assertEquals(
                icebergBucketHash,
                expectedHash,
                format("expected Iceberg %s(%s) bucket with %sd buckets to be %d, got %d", icebergType, icebergValue, Integer.MAX_VALUE, expectedHash, icebergBucketHash));

        // Ensure hash is stable and does not change
        assertEquals(
                trinoBucketHash,
                expectedHash,
                format("expected Trino %s(%s) bucket with %sd buckets to be %d, got %d", icebergType, icebergValue, Integer.MAX_VALUE, expectedHash, trinoBucketHash));
    }

    private Integer computeIcebergBucket(Type type, Object icebergValue, int bucketCount)
    {
        Transform<Object, Integer> bucketTransform = Transforms.bucket(type, bucketCount);
        assertTrue(bucketTransform.canTransform(type), format("bucket function %s is not able to transform type %s", bucketTransform, type));
        return bucketTransform.apply(icebergValue);
    }

    private Integer computeTrinoBucket(Type icebergType, Object icebergValue, int bucketCount)
    {
        io.trino.spi.type.Type trinoType = toTrinoType(icebergType, TYPE_MANAGER);
        Function<Block, Block> bucketTransform = getBucketTransform(trinoType, bucketCount);

        BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, 1);

        Object trinoValue = toTrinoValue(icebergType, icebergValue);
        verify(trinoValue == null || Primitives.wrap(trinoType.getJavaType()).isInstance(trinoValue), "Unexpected value for %s: %s", trinoType, trinoValue != null ? trinoValue.getClass() : null);
        writeNativeValue(trinoType, blockBuilder, trinoValue);
        Block block = blockBuilder.build();

        Block bucketBlock = bucketTransform.apply(block);
        verify(bucketBlock.getPositionCount() == 1);
        return bucketBlock.isNull(0) ? null : bucketBlock.getInt(0, 0);
    }

    private static Object toTrinoValue(Type icebergType, Object icebergValue)
    {
        io.trino.spi.type.Type trinoType = toTrinoType(icebergType, TYPE_MANAGER);

        if (icebergValue == null) {
            return null;
        }

        if (trinoType == INTEGER) {
            return (long) (int) icebergValue;
        }

        if (trinoType == BIGINT) {
            //noinspection RedundantCast
            return (long) icebergValue;
        }

        if (isShortDecimal(trinoType)) {
            return Decimals.encodeShortScaledValue((BigDecimal) icebergValue, ((io.trino.spi.type.DecimalType) trinoType).getScale());
        }

        if (isLongDecimal(trinoType)) {
            return Decimals.encodeScaledValue((BigDecimal) icebergValue, ((io.trino.spi.type.DecimalType) trinoType).getScale());
        }

        if (trinoType == VARCHAR) {
            return utf8Slice((String) icebergValue);
        }

        if (trinoType == VARBINARY) {
            return wrappedBuffer(((ByteBuffer) icebergValue).array());
        }

        throw new UnsupportedOperationException("Unsupported type: " + trinoType);
    }
}
