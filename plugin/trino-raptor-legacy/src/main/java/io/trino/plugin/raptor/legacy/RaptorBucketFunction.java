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
package io.trino.plugin.raptor.legacy;

import io.airlift.slice.XxHash64;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;

public class RaptorBucketFunction
        implements BucketFunction
{
    private final HashFunction[] functions;
    private final int bucketCount;

    public RaptorBucketFunction(int bucketCount, List<Type> types)
    {
        checkArgument(bucketCount > 0, "bucketCount must be at least one");
        this.bucketCount = bucketCount;
        this.functions = types.stream()
                .map(RaptorBucketFunction::getHashFunction)
                .toArray(HashFunction[]::new);
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            long value = functions[i].hash(block, position);
            hash = (hash * 31) + value;
        }
        int value = (int) (hash & Integer.MAX_VALUE);
        return value % bucketCount;
    }

    public static void validateBucketType(Type type)
    {
        getHashFunction(type);
    }

    private static HashFunction getHashFunction(Type type)
    {
        if (type.equals(BIGINT)) {
            return bigintHashFunction();
        }
        if (type.equals(INTEGER)) {
            return intHashFunction();
        }
        if (type instanceof VarcharType) {
            return varcharHashFunction();
        }
        throw new TrinoException(NOT_SUPPORTED, "Bucketing is supported for bigint, integer and varchar, not " + type.getDisplayName());
    }

    private static HashFunction bigintHashFunction()
    {
        return (block, position) -> XxHash64.hash(BIGINT.getLong(block, position));
    }

    private static HashFunction intHashFunction()
    {
        return (block, position) -> XxHash64.hash(INTEGER.getInt(block, position));
    }

    private static HashFunction varcharHashFunction()
    {
        return (block, position) -> XxHash64.hash(block.getSlice(position, 0, block.getSliceLength(position)));
    }

    private interface HashFunction
    {
        long hash(Block block, int position);
    }
}
