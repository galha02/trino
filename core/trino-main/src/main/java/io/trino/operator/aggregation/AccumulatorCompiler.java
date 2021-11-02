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
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.airlift.slice.Slice;
import io.trino.metadata.BoundSignature;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.CompilerOperations;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AccumulatorCompiler
{
    private AccumulatorCompiler() {}

    public static GenericAccumulatorFactoryBinder generateAccumulatorFactoryBinder(BoundSignature boundSignature, AggregationMetadata metadata)
    {
        // change types used in Aggregation methods to types used in the core Trino engine to simplify code generation
        metadata = normalizeAggregationMethods(boundSignature, metadata);

        DynamicClassLoader classLoader = new DynamicClassLoader(AccumulatorCompiler.class.getClassLoader());

        Class<? extends Accumulator> accumulatorClass = generateAccumulatorClass(
                boundSignature,
                Accumulator.class,
                metadata,
                classLoader);

        Class<? extends GroupedAccumulator> groupedAccumulatorClass = generateAccumulatorClass(
                boundSignature,
                GroupedAccumulator.class,
                metadata,
                classLoader);

        return new GenericAccumulatorFactoryBinder(
                accumulatorClass,
                metadata.getRemoveInputFunction().isPresent(),
                groupedAccumulatorClass);
    }

    private static <T> Class<? extends T> generateAccumulatorClass(
            BoundSignature boundSignature,
            Class<T> accumulatorInterface,
            AggregationMetadata metadata,
            DynamicClassLoader classLoader)
    {
        boolean grouped = accumulatorInterface == GroupedAccumulator.class;

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(boundSignature.getName() + accumulatorInterface.getSimpleName()),
                type(Object.class),
                type(accumulatorInterface));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        List<AccumulatorStateDescriptor<?>> stateDescriptors = metadata.getAccumulatorStateDescriptors();
        List<StateFieldAndDescriptor> stateFieldAndDescriptors = new ArrayList<>();
        for (int i = 0; i < stateDescriptors.size(); i++) {
            stateFieldAndDescriptors.add(new StateFieldAndDescriptor(
                    stateDescriptors.get(i),
                    definition.declareField(a(PRIVATE, FINAL), "stateSerializer_" + i, AccumulatorStateSerializer.class),
                    definition.declareField(a(PRIVATE, FINAL), "stateFactory_" + i, AccumulatorStateFactory.class),
                    definition.declareField(a(PRIVATE, FINAL), "state_" + i, grouped ? GroupedAccumulatorState.class : AccumulatorState.class)));
        }
        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        int lambdaCount = metadata.getLambdaInterfaces().size();
        List<FieldDefinition> lambdaProviderFields = new ArrayList<>(lambdaCount);
        for (int i = 0; i < lambdaCount; i++) {
            lambdaProviderFields.add(definition.declareField(a(PRIVATE, FINAL), "lambdaProvider_" + i, LambdaProvider.class));
        }

        FieldDefinition maskChannelField = definition.declareField(a(PRIVATE, FINAL), "maskChannel", int.class);

        int inputChannelCount = countInputChannels(metadata.getInputParameterKinds());
        ImmutableList.Builder<FieldDefinition> inputFieldsBuilder = ImmutableList.builderWithExpectedSize(inputChannelCount);
        for (int i = 0; i < inputChannelCount; i++) {
            inputFieldsBuilder.add(definition.declareField(a(PRIVATE, FINAL), "inputChannel" + i, int.class));
        }
        List<FieldDefinition> inputChannelFields = inputFieldsBuilder.build();

        // Generate constructor
        generateConstructor(
                definition,
                stateFieldAndDescriptors,
                inputChannelFields,
                maskChannelField,
                lambdaProviderFields,
                callSiteBinder,
                grouped);

        generatePrivateConstructor(definition);

        // Generate methods
        generateCopy(definition, stateFieldAndDescriptors, lambdaProviderFields, maskChannelField, inputChannelFields);

        generateAddInput(
                definition,
                stateFields,
                inputChannelFields,
                maskChannelField,
                boundSignature.getArgumentTypes(),
                metadata.getInputParameterKinds(),
                lambdaProviderFields,
                metadata.getInputFunction(),
                callSiteBinder,
                grouped);
        generateAddOrRemoveInputWindowIndex(
                definition,
                stateFields,
                metadata.getInputParameterKinds(),
                lambdaProviderFields,
                metadata.getInputFunction(),
                "addInput",
                callSiteBinder);
        metadata.getRemoveInputFunction().ifPresent(
                removeInputFunction -> generateAddOrRemoveInputWindowIndex(
                        definition,
                        stateFields,
                        metadata.getInputParameterKinds(),
                        lambdaProviderFields,
                        removeInputFunction,
                        "removeInput",
                        callSiteBinder));
        generateGetEstimatedSize(definition, stateFields);

        generateGetIntermediateType(
                definition,
                callSiteBinder,
                stateDescriptors.stream()
                        .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                        .collect(toImmutableList()));

        generateGetFinalType(definition, callSiteBinder, boundSignature.getReturnType());

        generateAddIntermediateAsCombine(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                metadata.getCombineFunction(),
                callSiteBinder,
                grouped);

        if (grouped) {
            generateGroupedEvaluateIntermediate(definition, stateFieldAndDescriptors);
        }
        else {
            generateEvaluateIntermediate(definition, stateFieldAndDescriptors);
        }

        if (grouped) {
            generateGroupedEvaluateFinal(definition, stateFields, metadata.getOutputFunction(), callSiteBinder);
        }
        else {
            generateEvaluateFinal(definition, stateFields, metadata.getOutputFunction(), callSiteBinder);
        }

        if (grouped) {
            generatePrepareFinal(definition);
        }
        return defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
    }

    private static void generateGetIntermediateType(ClassDefinition definition, CallSiteBinder callSiteBinder, List<Type> type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getIntermediateType", type(Type.class));

        if (type.size() == 1) {
            methodDefinition.getBody()
                    .append(constantType(callSiteBinder, getOnlyElement(type)))
                    .retObject();
        }
        else {
            methodDefinition.getBody()
                    .append(constantType(callSiteBinder, RowType.anonymous(type)))
                    .retObject();
        }
    }

    private static void generateGetFinalType(ClassDefinition definition, CallSiteBinder callSiteBinder, Type type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getFinalType", type(Type.class));

        methodDefinition.getBody()
                .append(constantType(callSiteBinder, type))
                .retObject();
    }

    private static void generateGetEstimatedSize(ClassDefinition definition, List<FieldDefinition> stateFields)
    {
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        Variable estimatedSize = method.getScope().declareVariable(long.class, "estimatedSize");
        method.getBody().append(estimatedSize.set(constantLong(0L)));

        for (FieldDefinition stateField : stateFields) {
            method.getBody()
                    .append(estimatedSize.set(
                            BytecodeExpressions.add(
                                    estimatedSize,
                                    method.getThis().getField(stateField).invoke("getEstimatedSize", long.class))));
        }
        method.getBody().append(estimatedSize.ret());
    }

    private static void generateAddInput(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            List<FieldDefinition> inputChannelFields,
            FieldDefinition maskChannelField,
            List<Type> inputTypes,
            List<AggregationParameterKind> parameterKinds,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        Parameter page = arg("page", Page.class);
        parameters.add(page);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        Variable masksBlock = scope.declareVariable(Block.class, "masksBlock");
        body.comment("masksBlock = extractMaskBlock(%s, page);", maskChannelField.getName())
                .append(thisVariable.getField(maskChannelField))
                .append(page)
                .invokeStatic(AggregationUtils.class, "extractMaskBlock", Block.class, int.class, Page.class)
                .putVariable(masksBlock);

        ImmutableList.Builder<Variable> variablesBuilder = ImmutableList.builderWithExpectedSize(inputChannelFields.size());
        for (int i = 0; i < inputChannelFields.size(); i++) {
            FieldDefinition inputChannelField = inputChannelFields.get(i);
            Variable blockVariable = scope.declareVariable(Block.class, "block" + i);
            variablesBuilder.add(blockVariable);
            body.comment("%s = page.getBlock(%s);", blockVariable.getName(), inputChannelField.getName())
                    .append(page)
                    .append(thisVariable.getField(inputChannelField))
                    .invokeVirtual(Page.class, "getBlock", Block.class, int.class)
                    .putVariable(blockVariable);
        }
        List<Variable> parameterVariables = variablesBuilder.build();

        BytecodeBlock block = generateInputForLoop(
                stateField,
                inputTypes,
                parameterKinds,
                inputFunction,
                scope,
                parameterVariables,
                lambdaProviderFields,
                masksBlock,
                callSiteBinder,
                grouped);

        body.append(block);
        body.ret();
    }

    private static void generateAddOrRemoveInputWindowIndex(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            List<AggregationParameterKind> parameterKinds,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            String generatedFunctionName,
            CallSiteBinder callSiteBinder)
    {
        // TODO: implement masking based on maskChannel field once Window Functions support DISTINCT arguments to the functions.

        Parameter index = arg("index", WindowIndex.class);
        Parameter channels = arg("channels", type(List.class, Integer.class));
        Parameter startPosition = arg("startPosition", int.class);
        Parameter endPosition = arg("endPosition", int.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                generatedFunctionName,
                type(void.class),
                ImmutableList.of(index, channels, startPosition, endPosition));
        Scope scope = method.getScope();

        Variable position = scope.declareVariable(int.class, "position");

        Binding binding = callSiteBinder.bind(inputFunction);
        BytecodeExpression invokeInputFunction = invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                generatedFunctionName,
                binding.getType(),
                getInvokeFunctionOnWindowIndexParameters(
                        scope,
                        inputFunction.type().parameterArray(),
                        parameterKinds,
                        lambdaProviderFields,
                        stateField,
                        index,
                        channels,
                        position));

        method.getBody()
                .append(new ForLoop()
                        .initialize(position.set(startPosition))
                        .condition(BytecodeExpressions.lessThanOrEqual(position, endPosition))
                        .update(position.increment())
                        .body(new IfStatement()
                                .condition(anyParametersAreNull(parameterKinds, index, channels, position))
                                .ifFalse(invokeInputFunction)))
                .ret();
    }

    private static BytecodeExpression anyParametersAreNull(
            List<AggregationParameterKind> parameterKinds,
            Variable index,
            Variable channels,
            Variable position)
    {
        int inputChannel = 0;

        BytecodeExpression isNull = constantFalse();
        for (AggregationParameterKind parameterKind : parameterKinds) {
            switch (parameterKind) {
                case BLOCK_INPUT_CHANNEL:
                case INPUT_CHANNEL:
                    BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(inputChannel)).cast(int.class);
                    isNull = BytecodeExpressions.or(isNull, index.invoke("isNull", boolean.class, getChannel, position));
                    inputChannel++;
                    break;
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    inputChannel++;
                    break;
                case BLOCK_INDEX:
                case STATE:
                    // TODO
            }
        }

        return isNull;
    }

    private static List<BytecodeExpression> getInvokeFunctionOnWindowIndexParameters(
            Scope scope,
            Class<?>[] parameterTypes,
            List<AggregationParameterKind> parameterKinds,
            List<FieldDefinition> lambdaProviderFields,
            List<FieldDefinition> stateField,
            Variable index,
            Variable channels,
            Variable position)
    {
        int inputChannel = 0;
        int stateIndex = 0;
        verify(parameterTypes.length == parameterKinds.size() + lambdaProviderFields.size());
        List<BytecodeExpression> expressions = new ArrayList<>();

        for (int i = 0; i < parameterKinds.size(); i++) {
            AggregationParameterKind parameterKind = parameterKinds.get(i);
            Class<?> parameterType = parameterTypes[i];
            BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(inputChannel)).cast(int.class);
            switch (parameterKind) {
                case STATE:
                    expressions.add(scope.getThis().getField(stateField.get(stateIndex)));
                    stateIndex++;
                    break;
                case BLOCK_INDEX:
                    // index.getSingleValueBlock(channel, position) generates always a page with only one position
                    expressions.add(constantInt(0));
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    expressions.add(index.invoke(
                            "getSingleValueBlock",
                            Block.class,
                            getChannel,
                            position));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    if (parameterType == long.class) {
                        expressions.add(index.invoke("getLong", long.class, getChannel, position));
                    }
                    else if (parameterType == double.class) {
                        expressions.add(index.invoke("getDouble", double.class, getChannel, position));
                    }
                    else if (parameterType == boolean.class) {
                        expressions.add(index.invoke("getBoolean", boolean.class, getChannel, position));
                    }
                    else if (parameterType == Slice.class) {
                        expressions.add(index.invoke("getSlice", Slice.class, getChannel, position));
                    }
                    else {
                        BytecodeExpression expression = index.invoke("getObject", Object.class, getChannel, position);
                        if (parameterType != Object.class) {
                            expression = expression.cast(parameterType);
                        }
                        expressions.add(expression);
                    }

                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterKind);
            }
        }

        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            expressions.add(scope.getThis().getField(lambdaProviderField)
                    .invoke("getLambda", Object.class));
        }

        return expressions;
    }

    private static BytecodeBlock generateInputForLoop(
            List<FieldDefinition> stateField,
            List<Type> inputTypes,
            List<AggregationParameterKind> parameterKinds,
            MethodHandle inputFunction,
            Scope scope,
            List<Variable> parameterVariables,
            List<FieldDefinition> lambdaProviderFields,
            Variable masksBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable page = scope.getVariable("page");
        Variable positionVariable = scope.declareVariable(int.class, "position");
        Variable rowsVariable = scope.declareVariable(int.class, "rows");

        BytecodeBlock block = new BytecodeBlock()
                .append(page)
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);

        BytecodeNode loopBody = generateInvokeInputFunction(
                scope,
                stateField,
                positionVariable,
                inputTypes,
                parameterVariables,
                parameterKinds,
                lambdaProviderFields,
                inputFunction,
                callSiteBinder,
                grouped);

        //  Wrap with null checks
        List<Boolean> nullable = new ArrayList<>();
        for (AggregationParameterKind parameterKind : parameterKinds) {
            switch (parameterKind) {
                case INPUT_CHANNEL:
                case BLOCK_INPUT_CHANNEL:
                    nullable.add(false);
                    break;
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    nullable.add(true);
                    break;
                default: // do nothing
            }
        }
        checkState(nullable.size() == parameterVariables.size(), "Number of parameters does not match");
        for (int i = 0; i < parameterVariables.size(); i++) {
            if (!nullable.get(i)) {
                Variable variableDefinition = parameterVariables.get(i);
                loopBody = new IfStatement("if(!%s.isNull(position))", variableDefinition.getName())
                        .condition(new BytecodeBlock()
                                .getVariable(variableDefinition)
                                .getVariable(positionVariable)
                                .invokeInterface(Block.class, "isNull", boolean.class, int.class))
                        .ifFalse(loopBody);
            }
        }

        loopBody = new IfStatement("if(testMask(%s, position))", masksBlock.getName())
                .condition(new BytecodeBlock()
                        .getVariable(masksBlock)
                        .getVariable(positionVariable)
                        .invokeStatic(CompilerOperations.class, "testMask", boolean.class, Block.class, int.class))
                .ifTrue(loopBody);

        ForLoop forLoop = new ForLoop()
                .initialize(new BytecodeBlock().putVariable(positionVariable, 0))
                .condition(new BytecodeBlock()
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(loopBody);

        block.append(new IfStatement("if(!maskGuaranteedToFilterAllRows(%s, %s))", rowsVariable.getName(), masksBlock.getName())
                .condition(new BytecodeBlock()
                        .getVariable(rowsVariable)
                        .getVariable(masksBlock)
                        .invokeStatic(AggregationUtils.class, "maskGuaranteedToFilterAllRows", boolean.class, int.class, Block.class))
                .ifFalse(forLoop));

        return block;
    }

    private static BytecodeBlock generateInvokeInputFunction(
            Scope scope,
            List<FieldDefinition> stateField,
            Variable position,
            List<Type> inputTypes,
            List<Variable> parameterVariables,
            List<AggregationParameterKind> parameterKinds,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        BytecodeBlock block = new BytecodeBlock();

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(scope, stateField, block);
        }

        block.comment("Call input function with unpacked Block arguments");

        Class<?>[] parameters = inputFunction.type().parameterArray();
        int inputChannel = 0;
        int stateIndex = 0;
        verify(parameters.length == parameterKinds.size() + lambdaProviderFields.size());
        for (int i = 0; i < parameterKinds.size(); i++) {
            AggregationParameterKind parameterKind = parameterKinds.get(i);
            switch (parameterKind) {
                case STATE:
                    block.append(scope.getThis().getField(stateField.get(stateIndex)));
                    stateIndex++;
                    break;
                case BLOCK_INDEX:
                    block.getVariable(position);
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    block.getVariable(parameterVariables.get(inputChannel));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    BytecodeBlock getBlockBytecode = new BytecodeBlock()
                            .getVariable(parameterVariables.get(inputChannel));
                    pushStackType(scope, block, inputTypes.get(inputChannel), getBlockBytecode, parameters[i], callSiteBinder);
                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterKind);
            }
        }
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            block.append(scope.getThis().getField(lambdaProviderField)
                    .invoke("getLambda", Object.class));
        }

        block.append(invoke(callSiteBinder.bind(inputFunction), "input"));
        return block;
    }

    // Assumes that there is a variable named 'position' in the block, which is the current index
    private static void pushStackType(Scope scope, BytecodeBlock block, Type sqlType, BytecodeBlock getBlockBytecode, Class<?> parameter, CallSiteBinder callSiteBinder)
    {
        Variable position = scope.getVariable("position");
        if (parameter == long.class) {
            block.comment("%s.getLong(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getLong", long.class, Block.class, int.class);
        }
        else if (parameter == double.class) {
            block.comment("%s.getDouble(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getDouble", double.class, Block.class, int.class);
        }
        else if (parameter == boolean.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getBoolean", boolean.class, Block.class, int.class);
        }
        else if (parameter == Slice.class) {
            block.comment("%s.getSlice(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getSlice", Slice.class, Block.class, int.class);
        }
        else {
            block.comment("%s.getObject(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getObject", Object.class, Block.class, int.class);
            if (sqlType.getJavaType() != Object.class) {
                block.checkCast(sqlType.getJavaType());
            }
        }
    }

    private static void generateAddIntermediateAsCombine(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle combineFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        MethodDefinition method = declareAddIntermediate(definition, grouped);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        int stateCount = stateFieldAndDescriptors.size();
        List<Variable> scratchStates = new ArrayList<>();
        for (int i = 0; i < stateCount; i++) {
            Class<?> scratchStateClass = AccumulatorState.class;
            scratchStates.add(scope.declareVariable(scratchStateClass, "scratchState_" + i));
        }

        List<Variable> block;
        if (stateCount == 1) {
            block = ImmutableList.of(scope.getVariable("block"));
        }
        else {
            // ColumnarRow is used to get the column blocks represents each state, this allows to
            //  1. handle single state and multiple states in a unified way
            //  2. avoid the cost of constructing SingleRowBlock for each group
            Variable columnarRow = scope.declareVariable(ColumnarRow.class, "columnarRow");
            body.append(columnarRow.set(
                    invokeStatic(ColumnarRow.class, "toColumnarRow", ColumnarRow.class, scope.getVariable("block"))));

            block = new ArrayList<>();
            for (int i = 0; i < stateCount; i++) {
                Variable columnBlock = scope.declareVariable(Block.class, "columnBlock_" + i);
                body.append(columnBlock.set(
                        columnarRow.invoke("getField", Block.class, constantInt(i))));
                block.add(columnBlock);
            }
        }

        Variable position = scope.declareVariable(int.class, "position");
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateFactoryField = stateFieldAndDescriptors.get(i).getStateFactoryField();
            body.comment(format("scratchState_%s = stateFactory[%s].createSingleState();", i, i))
                    .append(thisVariable.getField(stateFactoryField))
                    .invokeInterface(AccumulatorStateFactory.class, "createSingleState", AccumulatorState.class)
                    .checkCast(scratchStates.get(i).getType())
                    .putVariable(scratchStates.get(i));
        }

        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        if (grouped) {
            generateEnsureCapacity(scope, stateFields, body);
        }

        BytecodeBlock loopBody = new BytecodeBlock();

        loopBody.comment("combine(state_0, state_1, ... scratchState_0, scratchState_1, ... lambda_0, lambda_1, ...)");
        for (FieldDefinition stateField : stateFields) {
            if (grouped) {
                Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
                loopBody.append(thisVariable.getField(stateField).invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
            }
            loopBody.append(thisVariable.getField(stateField));
        }
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateSerializerField = stateFieldAndDescriptors.get(i).getStateSerializerField();
            loopBody.append(thisVariable.getField(stateSerializerField).invoke("deserialize", void.class, block.get(i), position, scratchStates.get(i).cast(AccumulatorState.class)));
            loopBody.append(scratchStates.get(i));
        }
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            loopBody.append(scope.getThis().getField(lambdaProviderField)
                    .invoke("getLambda", Object.class));
        }
        loopBody.append(invoke(callSiteBinder.bind(combineFunction), "combine"));

        if (grouped) {
            // skip rows with null group id
            IfStatement ifStatement = new IfStatement("if (!groupIdsBlock.isNull(position))")
                    .condition(not(scope.getVariable("groupIdsBlock").invoke("isNull", boolean.class, position)))
                    .ifTrue(loopBody);

            loopBody = new BytecodeBlock().append(ifStatement);
        }

        body.append(generateBlockNonNullPositionForLoop(scope, position, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIdsBlock(Scope scope, List<FieldDefinition> stateFields, BytecodeBlock block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        Variable position = scope.getVariable("position");
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = scope.getThis().getField(stateField);
            block.append(state.invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
        }
    }

    private static void generateEnsureCapacity(Scope scope, List<FieldDefinition> stateFields, BytecodeBlock block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = scope.getThis().getField(stateField);
            block.append(state.invoke("ensureCapacity", void.class, groupIdsBlock.invoke("getGroupCount", long.class)));
        }
    }

    private static MethodDefinition declareAddIntermediate(ClassDefinition definition, boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("block", Block.class));

        return definition.declareMethod(
                a(PUBLIC),
                "addIntermediate",
                type(void.class),
                parameters.build());
    }

    // Generates a for-loop with a local variable named "position" defined, with the current position in the block,
    // loopBody will only be executed for non-null positions in the Block
    private static BytecodeBlock generateBlockNonNullPositionForLoop(Scope scope, Variable positionVariable, BytecodeBlock loopBody)
    {
        Variable rowsVariable = scope.declareVariable(int.class, "rows");
        Variable blockVariable = scope.getVariable("block");

        BytecodeBlock block = new BytecodeBlock()
                .append(blockVariable)
                .invokeInterface(Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatement ifStatement = new IfStatement("if(!block.isNull(position))")
                .condition(new BytecodeBlock()
                        .append(blockVariable)
                        .append(positionVariable)
                        .invokeInterface(Block.class, "isNull", boolean.class, int.class))
                .ifFalse(loopBody);

        block.append(new ForLoop()
                .initialize(positionVariable.set(constantInt(0)))
                .condition(new BytecodeBlock()
                        .append(positionVariable)
                        .append(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(ifStatement));

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateIntermediate", type(void.class), groupId, out);

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                    .append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), out))
                    .ret();
        }
        else {
            Variable rowBuilder = method.getScope().declareVariable(BlockBuilder.class, "rowBuilder");
            body.append(rowBuilder.set(out.invoke("beginBlockEntry", BlockBuilder.class)));

            for (StateFieldAndDescriptor stateFieldAndDescriptor : stateFieldAndDescriptors) {
                BytecodeExpression stateSerializer = thisVariable.getField(stateFieldAndDescriptor.getStateSerializerField());
                BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptor.getStateField());

                body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                        .append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), rowBuilder));
            }
            body.append(out.invoke("closeEntry", BlockBuilder.class).pop())
                    .ret();
        }
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                out);

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), out))
                    .ret();
        }
        else {
            Variable rowBuilder = method.getScope().declareVariable(BlockBuilder.class, "rowBuilder");
            body.append(rowBuilder.set(out.invoke("beginBlockEntry", BlockBuilder.class)));

            for (StateFieldAndDescriptor stateFieldAndDescriptor : stateFieldAndDescriptors) {
                BytecodeExpression stateSerializer = thisVariable.getField(stateFieldAndDescriptor.getStateSerializerField());
                BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptor.getStateField());
                body.append(stateSerializer.invoke("serialize", void.class, state.cast(AccumulatorState.class), rowBuilder));
            }
            body.append(out.invoke("closeEntry", BlockBuilder.class).pop())
                    .ret();
        }
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateFinal", type(void.class), groupId, out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generatePrepareFinal(ClassDefinition definition)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "prepareFinal",
                type(void.class));
        method.getBody().ret();
    }

    private static void generateConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> inputChannelFields,
            FieldDefinition maskChannelField,
            List<FieldDefinition> lambdaProviderFields,
            CallSiteBinder callSiteBinder, boolean grouped)
    {
        Parameter inputChannels = arg("inputChannels", type(List.class, Integer.class));
        Parameter maskChannel = arg("maskChannel", type(Optional.class, Integer.class));
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, LambdaProvider.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                inputChannels,
                maskChannel,
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        for (StateFieldAndDescriptor fieldAndDescriptor : stateFieldAndDescriptors) {
            AccumulatorStateDescriptor<?> accumulatorStateDescriptor = fieldAndDescriptor.getAccumulatorStateDescriptor();
            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateSerializerField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getSerializer(), AccumulatorStateSerializer.class)));
            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateFactoryField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getFactory(), AccumulatorStateFactory.class)));
        }
        for (int i = 0; i < lambdaProviderFields.size(); i++) {
            body.append(thisVariable.setField(
                    lambdaProviderFields.get(i),
                    lambdaProviders.invoke("get", Object.class, constantInt(i))
                            .cast(LambdaProvider.class)));
        }

        body.append(thisVariable.setField(maskChannelField, invokeStatic(type(CompilerOperations.class), "optionalChannelToIntOrNegative", type(int.class), ImmutableList.of(type(Optional.class, Integer.class)), generateRequireNotNull(maskChannel))));
        // Validate inputChannels argument list
        body.append(generateRequireNotNull(inputChannels))
                .push(inputChannelFields.size())
                .invokeStatic(type(CompilerOperations.class), "validateChannelsListLength", type(void.class), ImmutableList.of(type(List.class, Integer.class), type(int.class)));
        // Initialize inputChannels fields
        if (!inputChannelFields.isEmpty()) {
            // Assign each inputChannel field from the list or assign -1 if the input list is empty (eg: intermediate aggregations don't use the input parameters)
            BytecodeBlock assignFromList = new BytecodeBlock().comment("(this.inputChannel = %s.get(i), ...)", inputChannels.getName());
            BytecodeBlock assignNegative = new BytecodeBlock().comment("(this.inputChannel = -1, ...)");
            for (int i = 0; i < inputChannelFields.size(); i++) {
                FieldDefinition inputChannelField = inputChannelFields.get(i);
                assignFromList.append(thisVariable.setField(
                        inputChannelField,
                        inputChannels.invoke("get", Object.class, constantInt(i)).cast(int.class)));
                assignNegative.append(thisVariable.setField(inputChannelField, constantInt(-1)));
            }
            body.append(new IfStatement("if(%s.isEmpty())", inputChannels.getName())
                    .condition(new BytecodeBlock().append(inputChannels).invokeInterface(List.class, "isEmpty", boolean.class))
                    .ifTrue(assignNegative)
                    .ifFalse(assignFromList));
        }

        String createState;
        if (grouped) {
            createState = "createGroupedState";
        }
        else {
            createState = "createSingleState";
        }

        for (StateFieldAndDescriptor stateFieldAndDescriptor : stateFieldAndDescriptors) {
            FieldDefinition stateField = stateFieldAndDescriptor.getStateField();
            BytecodeExpression stateFactory = thisVariable.getField(stateFieldAndDescriptor.getStateFactoryField());

            body.append(thisVariable.setField(stateField, stateFactory.invoke(createState, AccumulatorState.class).cast(stateField.getType())));
        }
        body.ret();
    }

    private static void generatePrivateConstructor(ClassDefinition definition)
    {
        MethodDefinition method = definition.declareConstructor(a(PRIVATE));

        BytecodeBlock body = method.getBody();
        body.comment("super();")
                .append(method.getThis())
                .invokeConstructor(Object.class)
                .ret();
    }

    private static void generateCopy(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldsAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            FieldDefinition maskChannelField,
            List<FieldDefinition> inputChannelFields)
    {
        MethodDefinition copy = definition.declareMethod(a(PUBLIC), "copy", type(Accumulator.class));
        Variable thisVariable = copy.getThis();

        Variable instanceCopy = copy.getScope().declareVariable(definition.getType(), "instanceCopy");
        copy.getBody().append(instanceCopy.set(newInstance(definition.getType())));

        for (StateFieldAndDescriptor descriptor : stateFieldsAndDescriptors) {
            FieldDefinition stateSerializerField = descriptor.getStateSerializerField();
            FieldDefinition stateFactoryField = descriptor.getStateFactoryField();
            FieldDefinition stateField = descriptor.getStateField();
            copy.getBody()
                    .append(instanceCopy.setField(stateSerializerField, thisVariable.getField(stateSerializerField)))
                    .append(instanceCopy.setField(stateFactoryField, thisVariable.getField(stateFactoryField)))
                    .append(instanceCopy.setField(stateField, thisVariable.getField(stateField).invoke("copy", AccumulatorState.class, ImmutableList.of()).cast(stateField.getType())));
        }

        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            copy.getBody()
                    .append(instanceCopy.setField(lambdaProviderField, thisVariable.getField(lambdaProviderField)));
        }

        copy.getBody()
                .append(instanceCopy.setField(maskChannelField, thisVariable.getField(maskChannelField)));

        for (FieldDefinition inputChannelField : inputChannelFields) {
            copy.getBody()
                    .append(instanceCopy.setField(inputChannelField, thisVariable.getField(inputChannelField)));
        }

        copy.getBody()
                .append(instanceCopy.ret());
    }

    private static BytecodeExpression generateRequireNotNull(Variable variable)
    {
        return invokeStatic(Objects.class, "requireNonNull", Object.class, variable.cast(Object.class), constantString(variable.getName() + " is null"))
                .cast(variable.getType());
    }

    private static int countInputChannels(List<AggregationParameterKind> parameterKinds)
    {
        int parameters = 0;
        for (AggregationParameterKind parameterKind : parameterKinds) {
            if (parameterKind == INPUT_CHANNEL ||
                    parameterKind == BLOCK_INPUT_CHANNEL ||
                    parameterKind == NULLABLE_BLOCK_INPUT_CHANNEL) {
                parameters++;
            }
        }

        return parameters;
    }

    private static AggregationMetadata normalizeAggregationMethods(BoundSignature boundSignature, AggregationMetadata metadata)
    {
        // change aggregations state variables to simply AccumulatorState to avoid any class loader issues in generated code
        int stateParameterCount = metadata.getAccumulatorStateDescriptors().size();
        int lambdaParameterCount = metadata.getLambdaInterfaces().size();
        return new AggregationMetadata(
                boundSignature,
                metadata.getInputParameterKinds(),
                castStateParameters(metadata.getInputFunction(), stateParameterCount, lambdaParameterCount),
                metadata.getRemoveInputFunction().map(removeFunction -> castStateParameters(removeFunction, stateParameterCount, lambdaParameterCount)),
                castStateParameters(metadata.getCombineFunction(), stateParameterCount * 2, lambdaParameterCount),
                castStateParameters(metadata.getOutputFunction(), stateParameterCount, 0),
                metadata.getAccumulatorStateDescriptors(),
                metadata.getLambdaInterfaces());
    }

    private static MethodHandle castStateParameters(MethodHandle inputFunction, int stateParameterCount, int lambdaParameterCount)
    {
        Class<?>[] parameterTypes = inputFunction.type().parameterArray();
        for (int i = 0; i < stateParameterCount; i++) {
            parameterTypes[i] = AccumulatorState.class;
        }
        for (int i = parameterTypes.length - lambdaParameterCount; i < parameterTypes.length; i++) {
            parameterTypes[i] = Object.class;
        }
        return MethodHandles.explicitCastArguments(inputFunction, MethodType.methodType(inputFunction.type().returnType(), parameterTypes));
    }

    private static class StateFieldAndDescriptor
    {
        private final AccumulatorStateDescriptor<?> accumulatorStateDescriptor;
        private final FieldDefinition stateSerializerField;
        private final FieldDefinition stateFactoryField;
        private final FieldDefinition stateField;

        private StateFieldAndDescriptor(AccumulatorStateDescriptor<?> accumulatorStateDescriptor, FieldDefinition stateSerializerField, FieldDefinition stateFactoryField, FieldDefinition stateField)
        {
            this.accumulatorStateDescriptor = accumulatorStateDescriptor;
            this.stateSerializerField = requireNonNull(stateSerializerField, "stateSerializerField is null");
            this.stateFactoryField = requireNonNull(stateFactoryField, "stateFactoryField is null");
            this.stateField = requireNonNull(stateField, "stateField is null");
        }

        public AccumulatorStateDescriptor<?> getAccumulatorStateDescriptor()
        {
            return accumulatorStateDescriptor;
        }

        private FieldDefinition getStateSerializerField()
        {
            return stateSerializerField;
        }

        private FieldDefinition getStateFactoryField()
        {
            return stateFactoryField;
        }

        private FieldDefinition getStateField()
        {
            return stateField;
        }
    }
}
