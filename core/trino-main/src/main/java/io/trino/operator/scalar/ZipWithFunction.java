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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.max;

public final class ZipWithFunction
        extends SqlScalarFunction
{
    public static final ZipWithFunction ZIP_WITH_FUNCTION = new ZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipWithFunction.class, "zipWith", Type.class, Type.class, ArrayType.class, Object.class, Block.class, Block.class, BinaryFunctionInterface.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(ZipWithFunction.class, "createState", ArrayType.class);

    private ZipWithFunction()
    {
        super(new FunctionMetadata(
                Signature.builder()
                        .name("zip_with")
                        .typeVariable("T")
                        .typeVariable("U")
                        .typeVariable("R")
                        .returnType(arrayType(new TypeSignature("R")))
                        .argumentType(arrayType(new TypeSignature("T")))
                        .argumentType(arrayType(new TypeSignature("U")))
                        .argumentType(functionType(new TypeSignature("T"), new TypeSignature("U"), new TypeSignature("R")))
                        .build(),
                new FunctionNullability(false, ImmutableList.of(false, false, false)),
                false,
                false,
                "Merge two arrays, element-wise, into a single array using the lambda function",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type leftElementType = ((ArrayType) boundSignature.getArgumentType(0)).getElementType();
        Type rightElementType = ((ArrayType) boundSignature.getArgumentType(1)).getElementType();
        Type outputElementType = ((ArrayType) boundSignature.getReturnType()).getElementType();
        ArrayType outputArrayType = new ArrayType(outputElementType);
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL, FUNCTION),
                ImmutableList.of(BinaryFunctionInterface.class),
                METHOD_HANDLE.bindTo(leftElementType).bindTo(rightElementType).bindTo(outputArrayType),
                Optional.of(STATE_FACTORY.bindTo(outputArrayType)));
    }

    public static Object createState(ArrayType arrayType)
    {
        return new PageBuilder(ImmutableList.of(arrayType));
    }

    public static Block zipWith(
            Type leftElementType,
            Type rightElementType,
            ArrayType outputArrayType,
            Object state,
            Block leftBlock,
            Block rightBlock,
            BinaryFunctionInterface function)
    {
        Type outputElementType = outputArrayType.getElementType();
        int leftPositionCount = leftBlock.getPositionCount();
        int rightPositionCount = rightBlock.getPositionCount();
        int outputPositionCount = max(leftPositionCount, rightPositionCount);

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder arrayBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = arrayBlockBuilder.beginBlockEntry();

        for (int position = 0; position < outputPositionCount; position++) {
            Object left = position < leftPositionCount ? readNativeValue(leftElementType, leftBlock, position) : null;
            Object right = position < rightPositionCount ? readNativeValue(rightElementType, rightBlock, position) : null;
            Object output;
            try {
                output = function.apply(left, right);
            }
            catch (Throwable throwable) {
                // Restore pageBuilder into a consistent state.
                arrayBlockBuilder.closeEntry();
                pageBuilder.declarePosition();

                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
            writeNativeValue(outputElementType, blockBuilder, output);
        }

        arrayBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return outputArrayType.getObject(arrayBlockBuilder, arrayBlockBuilder.getPositionCount() - 1);
    }
}
