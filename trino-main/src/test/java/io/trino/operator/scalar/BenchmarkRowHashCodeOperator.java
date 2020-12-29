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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.prestosql.operator.scalar.TypeOperatorBenchmarkUtil.addElement;
import static io.prestosql.operator.scalar.TypeOperatorBenchmarkUtil.getHashCodeBlockMethod;
import static io.prestosql.operator.scalar.TypeOperatorBenchmarkUtil.toType;
import static java.util.Collections.nCopies;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 30, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRowHashCodeOperator
{
    private static final int POSITIONS = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long hashOperator(BenchmarkData data)
            throws Throwable
    {
        return (long) data.getHashBlock().invokeExact(data.getBlock());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"BIGINT", "VARCHAR"})
        private String type = "BIGINT";

        @Param({"1", "8", "16", "32", "64", "128"})
        private int fieldCount = 32;

        private MethodHandle hashBlock;
        private Block block;

        @Setup
        public void setup()
        {
            RowType rowType = RowType.anonymous(ImmutableList.copyOf(nCopies(fieldCount, toType(type))));
            block = createChannel(POSITIONS, rowType);
            hashBlock = getHashCodeBlockMethod(rowType);
        }

        private static Block createChannel(int positionCount, RowType rowType)
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            BlockBuilder blockBuilder = rowType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

                List<Field> fields = rowType.getFields();
                for (Field field : fields) {
                    addElement(field.getType(), random, entryBuilder);
                }
                blockBuilder.closeEntry();
            }
            return blockBuilder.build();
        }

        public MethodHandle getHashBlock()
        {
            return hashBlock;
        }

        public Block getBlock()
        {
            return block;
        }
    }

    @Test
    public void test()
            throws Throwable
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        hashOperator(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkRowHashCodeOperator.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
