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
import io.trino.jmh.Benchmarks;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.VariableReferenceExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.FunctionType;
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

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.operator.scalar.BenchmarkArrayFilter.ExactArrayFilterFunction.EXACT_ARRAY_FILTER_FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Boolean.TRUE;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayFilter
{
    private static final int POSITIONS = 100_000;
    private static final int ARRAY_SIZE = 4;
    private static final int NUM_TYPES = 1;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);

    static {
        verify(NUM_TYPES == TYPES.size());
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS * ARRAY_SIZE * NUM_TYPES)
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.getPageProcessor().process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.getPage()));
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"filter", "exact_filter"})
        private String name = "filter";

        private Page page;
        private PageProcessor pageProcessor;

        @Setup
        public void setup()
        {
            TestingFunctionResolution functionResolution = new TestingFunctionResolution(InternalFunctionBundle.builder().function(EXACT_ARRAY_FILTER_FUNCTION).build());
            ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
            ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
            Block[] blocks = new Block[TYPES.size()];
            for (int i = 0; i < TYPES.size(); i++) {
                Type elementType = TYPES.get(i);
                ArrayType arrayType = new ArrayType(elementType);
                ResolvedFunction resolvedFunction = functionResolution.resolveFunction(
                        QualifiedName.of(name),
                        fromTypes(arrayType, new FunctionType(ImmutableList.of(BIGINT), BOOLEAN)));
                ResolvedFunction lessThan = functionResolution.resolveOperator(LESS_THAN, ImmutableList.of(BIGINT, BIGINT));
                projectionsBuilder.add(new CallExpression(resolvedFunction, ImmutableList.of(
                        field(0, arrayType),
                        new LambdaDefinitionExpression(
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("x"),
                                new CallExpression(lessThan, ImmutableList.of(constant(0L, BIGINT), new VariableReferenceExpression("x", BIGINT)))))));
                blocks[i] = createChannel(POSITIONS, ARRAY_SIZE, arrayType);
            }

            ImmutableList<RowExpression> projections = projectionsBuilder.build();
            pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
            page = new Page(blocks);
        }

        private static Block createChannel(int positionCount, int arraySize, ArrayType arrayType)
        {
            BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                for (int i = 0; i < arraySize; i++) {
                    if (arrayType.getElementType().getJavaType() == long.class) {
                        arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                blockBuilder.closeEntry();
            }
            return blockBuilder.build();
        }

        public PageProcessor getPageProcessor()
        {
            return pageProcessor;
        }

        public Page getPage()
        {
            return page;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkArrayFilter().benchmark(data);

        Benchmarks.benchmark(BenchmarkArrayFilter.class).run();
    }

    public static final class ExactArrayFilterFunction
            extends SqlScalarFunction
    {
        public static final ExactArrayFilterFunction EXACT_ARRAY_FILTER_FUNCTION = new ExactArrayFilterFunction();

        private static final MethodHandle METHOD_HANDLE = methodHandle(ExactArrayFilterFunction.class, "filter", Type.class, Block.class, MethodHandle.class);

        private ExactArrayFilterFunction()
        {
            super(new FunctionMetadata(
                    Signature.builder()
                            .name("exact_filter")
                            .typeVariable("T")
                            .returnType(arrayType(new TypeSignature("T")))
                            .argumentType(arrayType(new TypeSignature("T")))
                            .argumentType(functionType(new TypeSignature("T"), BOOLEAN.getTypeSignature()))
                            .build(),
                    new FunctionNullability(false, ImmutableList.of(false, false)),
                    false,
                    false,
                    "return array containing elements that match the given predicate",
                    SCALAR));
        }

        @Override
        protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
        {
            Type type = ((ArrayType) boundSignature.getReturnType()).getElementType();
            return new ChoicesScalarFunctionImplementation(
                    boundSignature,
                    FAIL_ON_NULL,
                    ImmutableList.of(NEVER_NULL, NEVER_NULL),
                    METHOD_HANDLE.bindTo(type));
        }

        public static Block filter(Type type, Block block, MethodHandle function)
        {
            int positionCount = block.getPositionCount();
            BlockBuilder resultBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                Long input = (Long) readNativeValue(type, block, position);
                Boolean keep;
                try {
                    keep = (Boolean) function.invokeExact(input);
                }
                catch (Throwable t) {
                    throwIfUnchecked(t);
                    throw new RuntimeException(t);
                }
                if (TRUE.equals(keep)) {
                    type.appendTo(block, position, resultBuilder);
                }
            }
            return resultBuilder.build();
        }
    }
}
