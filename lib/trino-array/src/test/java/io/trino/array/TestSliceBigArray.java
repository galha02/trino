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
package io.trino.array;

import io.airlift.slice.Slice;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.SizeOf.MEMORY_SEGMENT_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSliceBigArray
{
    private static final long BIG_ARRAY_INSTANCE_SIZE = instanceSize(SliceBigArray.class) + new ReferenceCountMap().sizeOf() + new ObjectBigArray<Slice>().sizeOf();
    private static final long SLICE_INSTANCE_SIZE = instanceSize(Slice.class) + MEMORY_SEGMENT_INSTANCE_SIZE;
    private static final int CAPACITY = 32;
    private final byte[] firstBytes = new byte[1234];
    private final byte[] secondBytes = new byte[4567];

    @Test
    public void testSameSliceRetainedSize()
    {
        SliceBigArray sliceBigArray = new SliceBigArray();
        sliceBigArray.ensureCapacity(CAPACITY);

        // same slice should be counted only once
        Slice slice = wrappedBuffer(secondBytes, 201, 1501);
        for (int i = 0; i < CAPACITY; i++) {
            sliceBigArray.set(i, slice);
            assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE);
        }

        // adding a new slice will increase the size
        slice = wrappedBuffer(secondBytes, 201, 1501);
        sliceBigArray.set(3, slice);
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 2);
    }

    @Test
    public void testNullSlicesRetainedSize()
    {
        SliceBigArray sliceBigArray = new SliceBigArray();
        sliceBigArray.ensureCapacity(CAPACITY);

        // add null values
        sliceBigArray.set(0, null);
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE);

        // replace null with a slice
        sliceBigArray.set(0, wrappedBuffer(secondBytes, 201, 1501));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE);

        // add another slice
        sliceBigArray.set(1, wrappedBuffer(secondBytes, 201, 1501));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 2);

        // replace slice with null
        sliceBigArray.set(1, null);
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE);
    }

    @Test
    public void testRetainedSize()
    {
        SliceBigArray sliceBigArray = new SliceBigArray();
        sliceBigArray.ensureCapacity(CAPACITY);

        // add two elements
        sliceBigArray.set(0, wrappedBuffer(firstBytes, 0, 100));
        sliceBigArray.set(1, wrappedBuffer(secondBytes, 0, 100));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(firstBytes) + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 2);

        // add two more
        sliceBigArray.set(2, wrappedBuffer(firstBytes, 100, 200));
        sliceBigArray.set(3, wrappedBuffer(secondBytes, 20, 150));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(firstBytes) + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 4);

        // replace with different slices but the same base
        sliceBigArray.set(2, wrappedBuffer(firstBytes, 11, 1200));
        sliceBigArray.set(3, wrappedBuffer(secondBytes, 201, 1501));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(firstBytes) + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 4);

        // replace with a different slice with a different base
        sliceBigArray.set(0, wrappedBuffer(secondBytes, 11, 1200));
        sliceBigArray.set(2, wrappedBuffer(secondBytes, 201, 1501));
        assertThat(sliceBigArray.sizeOf()).isEqualTo(BIG_ARRAY_INSTANCE_SIZE + sizeOf(secondBytes) + SLICE_INSTANCE_SIZE * 4);
    }
}
