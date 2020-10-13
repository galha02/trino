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
package io.prestosql.type;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.type.BlockTypeOperators.BlockPositionEqual;
import io.prestosql.type.BlockTypeOperators.BlockPositionHashCode;
import io.prestosql.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import io.prestosql.type.BlockTypeOperators.BlockPositionXxHash64;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.block.BlockSerdeUtil.writeBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.toRow;
import static io.prestosql.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.prestosql.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.function.InvocationConvention.simpleConvention;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.util.StructuralTestUtil.arrayBlockOf;
import static io.prestosql.util.StructuralTestUtil.mapBlockOf;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestType
{
    private final BlockEncodingSerde blockEncodingSerde = createTestMetadataManager().getBlockEncodingSerde();

    private final Class<?> objectValueType;
    private final Block testBlock;
    private final Type type;
    private final TypeOperators typeOperators;
    protected final BlockTypeOperators blockTypeOperators;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;
    private final BlockPositionXxHash64 xxHash64Operator;
    private final BlockPositionIsDistinctFrom distinctFromOperator;
    private final SortedMap<Integer, Object> expectedStackValues;
    private final SortedMap<Integer, Object> expectedObjectValues;
    private final Block testBlockWithNulls;

    protected AbstractTestType(Type type, Class<?> objectValueType, Block testBlock)
    {
        this(type, objectValueType, testBlock, testBlock);
    }

    protected AbstractTestType(Type type, Class<?> objectValueType, Block testBlock, Block expectedValues)
    {
        this.type = requireNonNull(type, "type is null");
        typeOperators = new TypeOperators();
        blockTypeOperators = new BlockTypeOperators(typeOperators);
        if (type.isComparable()) {
            equalOperator = blockTypeOperators.getEqualOperator(type);
            hashCodeOperator = blockTypeOperators.getHashCodeOperator(type);
            xxHash64Operator = blockTypeOperators.getXxHash64Operator(type);
            distinctFromOperator = blockTypeOperators.getDistinctFromOperator(type);
        }
        else {
            equalOperator = null;
            hashCodeOperator = null;
            xxHash64Operator = null;
            distinctFromOperator = null;
        }
        this.objectValueType = requireNonNull(objectValueType, "objectValueType is null");
        this.testBlock = requireNonNull(testBlock, "testBlock is null");

        requireNonNull(expectedValues, "expectedValues is null");
        this.expectedStackValues = indexStackValues(type, expectedValues);
        this.expectedObjectValues = indexObjectValues(type, expectedValues);
        this.testBlockWithNulls = createAlternatingNullsBlock(testBlock);
    }

    private Block createAlternatingNullsBlock(Block testBlock)
    {
        BlockBuilder nullsBlockBuilder = type.createBlockBuilder(null, testBlock.getPositionCount());
        for (int position = 0; position < testBlock.getPositionCount(); position++) {
            if (testBlock.isNull(position)) {
                checkState(type instanceof UnknownType);
                nullsBlockBuilder.appendNull();
            }
            else if (type.getJavaType() == boolean.class) {
                type.writeBoolean(nullsBlockBuilder, type.getBoolean(testBlock, position));
            }
            else if (type.getJavaType() == long.class) {
                type.writeLong(nullsBlockBuilder, type.getLong(testBlock, position));
            }
            else if (type.getJavaType() == double.class) {
                type.writeDouble(nullsBlockBuilder, type.getDouble(testBlock, position));
            }
            else if (type.getJavaType() == Slice.class) {
                Slice slice = type.getSlice(testBlock, position);
                type.writeSlice(nullsBlockBuilder, slice, 0, slice.length());
            }
            else {
                type.writeObject(nullsBlockBuilder, type.getObject(testBlock, position));
            }
            nullsBlockBuilder.appendNull();
        }
        return nullsBlockBuilder.build();
    }

    @Test
    public void testBlock()
    {
        for (Entry<Integer, Object> entry : expectedStackValues.entrySet()) {
            assertPositionEquals(testBlock, entry.getKey(), entry.getValue(), expectedObjectValues.get(entry.getKey()));
        }
        for (Entry<Integer, Object> entry : expectedStackValues.entrySet()) {
            assertPositionEquals(testBlockWithNulls, entry.getKey() * 2, entry.getValue(), expectedObjectValues.get(entry.getKey()));
            assertPositionEquals(testBlockWithNulls, (entry.getKey() * 2) + 1, null, null);
        }
    }

    protected void assertPositionEquals(Block block, int position, Object expectedStackValue, Object expectedObjectValue)
    {
        long hash = 0;
        if (type.isComparable()) {
            hash = hashCodeOperator.hashCodeNullSafe(block, position);
        }
        assertPositionValue(block, position, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(position, 1), 0, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedStackValue, hash, expectedObjectValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedStackValue, hash, expectedObjectValue);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.appendTo(block, position, blockBuilder);
        assertPositionValue(blockBuilder.build(), 0, expectedStackValue, hash, expectedObjectValue);
    }

    private void assertPositionValue(Block block, int position, Object expectedStackValue, long expectedHash, Object expectedObjectValue)
    {
        assertEquals(block.isNull(position), expectedStackValue == null);

        Object objectValue = type.getObjectValue(SESSION, block, position);
        assertEquals(objectValue, expectedObjectValue);
        if (objectValue != null) {
            assertInstanceOf(objectValue, objectValueType);
        }

        Block expectedBlock = createBlock(type, expectedStackValue);
        if (type.isComparable()) {
            assertTrue(equalOperator.equalNullSafe(block, position, block, position));
            assertTrue(equalOperator.equalNullSafe(block, position, expectedBlock, 0));
            assertTrue(equalOperator.equalNullSafe(expectedBlock, 0, block, position));
            assertEquals(hashCodeOperator.hashCodeNullSafe(block, position), expectedHash);
            assertFalse(distinctFromOperator.isDistinctFrom(block, position, block, position));
            assertFalse(distinctFromOperator.isDistinctFrom(block, position, expectedBlock, 0));
            assertFalse(distinctFromOperator.isDistinctFrom(expectedBlock, 0, block, position));
        }
        else {
            try {
                typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
                fail("Expected UnsupportedOperationException");
            }
            catch (UnsupportedOperationException expected) {
            }
            try {
                typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
                fail("Expected UnsupportedOperationException");
            }
            catch (UnsupportedOperationException expected) {
            }
            try {
                typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
                fail("Expected UnsupportedOperationException");
            }
            catch (UnsupportedOperationException expected) {
            }
        }

        assertEquals(block.isNull(position), expectedStackValue == null);

        if (type.isOrderable()) {
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, expectedBlock, 0) == 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, expectedBlock, 0) == 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, expectedBlock, 0) == 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, expectedBlock, 0) == 0);
        }
        else {
            try {
                typeOperators.getComparisonOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                fail("Expected UnsupportedOperationException");
            }
            catch (UnsupportedOperationException expected) {
            }
        }

        verifyInvalidPositionHandling(block);

        if (block.isNull(position)) {
            if (type.isOrderable() && !(type instanceof UnknownType)) {
                Block nonNullValue = toBlock(getNonNullValue());
                assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, nonNullValue, 0) < 0);
                assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, nonNullValue, 0) > 0);
                assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, nonNullValue, 0) < 0);
                assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, nonNullValue, 0) > 0);
            }
            return;
        }

        if (type.isOrderable() && expectedStackValue != Boolean.TRUE) {
            Block greaterValue = toBlock(getGreaterValue(expectedStackValue));
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, position, greaterValue, 0) < 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_LAST).order(block, position, greaterValue, 0) < 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_FIRST).order(block, position, greaterValue, 0) > 0);
            assertTrue(blockTypeOperators.generateBlockPositionOrdering(type, DESC_NULLS_LAST).order(block, position, greaterValue, 0) > 0);
        }

        if (type.getJavaType() == boolean.class) {
            assertEquals(type.getBoolean(block, position), expectedStackValue);
            try {
                type.getLong(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getDouble(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getObject(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == long.class) {
            assertEquals(type.getLong(block, position), expectedStackValue);
            try {
                type.getBoolean(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getDouble(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getObject(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == double.class) {
            assertEquals(type.getDouble(block, position), expectedStackValue);
            try {
                type.getBoolean(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getLong(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getObject(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == Slice.class) {
            assertEquals(type.getSlice(block, position), expectedStackValue);
            assertEquals(type.getObject(block, position), expectedStackValue);
            try {
                type.getBoolean(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getLong(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getDouble(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type.getJavaType() == Block.class) {
            SliceOutput actualSliceOutput = new DynamicSliceOutput(100);
            writeBlock(blockEncodingSerde, actualSliceOutput, (Block) type.getObject(block, position));
            SliceOutput expectedSliceOutput = new DynamicSliceOutput(actualSliceOutput.size());
            writeBlock(blockEncodingSerde, expectedSliceOutput, (Block) expectedStackValue);
            assertEquals(actualSliceOutput.slice(), expectedSliceOutput.slice());
            try {
                type.getBoolean(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getLong(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getDouble(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getSlice(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else {
            assertEquals(type.getObject(block, position), expectedStackValue);
            try {
                type.getBoolean(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getLong(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                type.getDouble(block, position);
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
    }

    private void verifyInvalidPositionHandling(Block block)
    {
        try {
            type.getObjectValue(SESSION, block, -1);
            fail("expected RuntimeException");
        }
        catch (RuntimeException expected) {
        }
        try {
            type.getObjectValue(SESSION, block, block.getPositionCount());
            fail("expected RuntimeException");
        }
        catch (RuntimeException expected) {
        }

        if (type.isComparable()) {
            try {
                hashCodeOperator.hashCode(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                hashCodeOperator.hashCode(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                xxHash64Operator.xxHash64(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                xxHash64Operator.xxHash64(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }

        if (type.isComparable() && !(type instanceof UnknownType)) {
            Block other = toBlock(getNonNullValue());
            try {
                equalOperator.equal(block, -1, other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                equalOperator.equal(block, block.getPositionCount(), other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }

            try {
                distinctFromOperator.isDistinctFrom(block, -1, other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                distinctFromOperator.isDistinctFrom(block, block.getPositionCount(), other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }

        if (type.isOrderable() && !(type instanceof UnknownType)) {
            Block other = toBlock(getNonNullValue());
            try {
                blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, -1, other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                blockTypeOperators.generateBlockPositionOrdering(type, ASC_NULLS_FIRST).order(block, block.getPositionCount(), other, 0);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }

        if (type.getJavaType() == boolean.class) {
            try {
                type.getBoolean(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                type.getBoolean(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }
        else if (type.getJavaType() == long.class) {
            try {
                type.getLong(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                type.getLong(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }
        else if (type.getJavaType() == double.class) {
            try {
                type.getDouble(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                type.getDouble(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }
        else if (type.getJavaType() == Slice.class) {
            try {
                type.getSlice(block, -1);
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
            try {
                type.getSlice(block, block.getPositionCount());
                fail("expected RuntimeException");
            }
            catch (RuntimeException expected) {
            }
        }
    }

    private static Block createBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);

        Class<?> javaType = type.getJavaType();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    /**
     * @param value value, represented in native container type
     * @return a value that is greater than input, represented in native container type
     */
    protected abstract Object getGreaterValue(Object value);

    /**
     * @return a non-null value, represented in native container type
     */
    protected Object getNonNullValue()
    {
        return getNonNullValueForType(type);
    }

    /**
     * @return a non-null value, represented in native container type
     */
    private static Object getNonNullValueForType(Type type)
    {
        if (type.getJavaType() == boolean.class) {
            return true;
        }
        if (type.getJavaType() == long.class) {
            return 1L;
        }
        if (type.getJavaType() == double.class) {
            return 1.0;
        }
        if (type.getJavaType() == Slice.class) {
            return Slices.utf8Slice("_");
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getElementType();
            Object elementNonNullValue = getNonNullValueForType(elementType);
            return arrayBlockOf(elementType, elementNonNullValue);
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            Object keyNonNullValue = getNonNullValueForType(keyType);
            Object valueNonNullValue = getNonNullValueForType(valueType);
            Map<?, ?> map = ImmutableMap.of(keyNonNullValue, valueNonNullValue);
            return mapBlockOf(keyType, valueType, map);
        }
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            List<Type> elementTypes = rowType.getTypeParameters();
            Object[] elementNonNullValues = elementTypes.stream().map(AbstractTestType::getNonNullValueForType).toArray(Object[]::new);
            return toRow(elementTypes, elementNonNullValues);
        }
        throw new IllegalStateException("Unsupported Java type " + type.getJavaType() + " (for type " + type + ")");
    }

    private Block toBlock(Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        Class<?> javaType = type.getJavaType();
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
        return blockBuilder.build();
    }

    private static SortedMap<Integer, Object> indexStackValues(Type type, Block block)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                values.put(position, null);
            }
            else if (type.getJavaType() == boolean.class) {
                values.put(position, type.getBoolean(block, position));
            }
            else if (type.getJavaType() == long.class) {
                values.put(position, type.getLong(block, position));
            }
            else if (type.getJavaType() == double.class) {
                values.put(position, type.getDouble(block, position));
            }
            else if (type.getJavaType() == Slice.class) {
                values.put(position, type.getSlice(block, position));
            }
            else {
                values.put(position, type.getObject(block, position));
            }
        }
        return unmodifiableSortedMap(values);
    }

    private static SortedMap<Integer, Object> indexObjectValues(Type type, Block block)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.put(position, type.getObjectValue(SESSION, block, position));
        }
        return unmodifiableSortedMap(values);
    }
}
