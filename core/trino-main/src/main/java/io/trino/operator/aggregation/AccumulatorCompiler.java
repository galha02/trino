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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionNullability;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.window.InternalWindowIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.WindowIndex;
import io.trino.sql.gen.Binding;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.gen.CompilerOperations;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.sql.gen.BytecodeUtils.invoke;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AccumulatorCompiler
{
    private AccumulatorCompiler() {}

    public static AccumulatorFactory generateAccumulatorFactory(
            BoundSignature boundSignature,
            AggregationMetadata metadata,
            FunctionNullability functionNullability,
            List<LambdaProvider> lambdaProviders)
    {
        // change types used in Aggregation methods to types used in the core Trino engine to simplify code generation
        metadata = normalizeAggregationMethods(metadata);

        DynamicClassLoader classLoader = new DynamicClassLoader(AccumulatorCompiler.class.getClassLoader());

        List<Boolean> argumentNullable = functionNullability.getArgumentNullable()
                .subList(0, functionNullability.getArgumentNullable().size() - metadata.getLambdaInterfaces().size());

        Constructor<? extends Accumulator> accumulatorConstructor = generateAccumulatorClass(
                boundSignature,
                Accumulator.class,
                metadata,
                argumentNullable,
                classLoader);

        Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor = generateAccumulatorClass(
                boundSignature,
                GroupedAccumulator.class,
                metadata,
                argumentNullable,
                classLoader);

        return new CompiledAccumulatorFactory(
                accumulatorConstructor,
                groupedAccumulatorConstructor,
                lambdaProviders);
    }

    private static <T> Constructor<? extends T> generateAccumulatorClass(
            BoundSignature boundSignature,
            Class<T> accumulatorInterface,
            AggregationMetadata metadata,
            List<Boolean> argumentNullable,
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

        // Generate constructor
        generateConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                callSiteBinder,
                grouped);

        generatePrivateConstructor(definition);

        // Generate methods
        generateCopy(definition, stateFieldAndDescriptors, lambdaProviderFields, Accumulator.class);

        generateAddInput(
                definition,
                stateFields,
                argumentNullable,
                lambdaProviderFields,
                metadata.getInputFunction(),
                callSiteBinder,
                grouped);
        generateGetEstimatedSize(definition, stateFields);

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

        Class<? extends T> accumulatorClass = defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
        try {
            return accumulatorClass.getConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static Constructor<? extends WindowAccumulator> generateWindowAccumulatorClass(
            BoundSignature boundSignature,
            AggregationMetadata metadata,
            FunctionNullability functionNullability)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AccumulatorCompiler.class.getClassLoader());

        List<Boolean> argumentNullable = functionNullability.getArgumentNullable()
                .subList(0, functionNullability.getArgumentNullable().size() - metadata.getLambdaInterfaces().size());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(boundSignature.getName() + WindowAccumulator.class.getSimpleName()),
                type(Object.class),
                type(WindowAccumulator.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        List<AccumulatorStateDescriptor<?>> stateDescriptors = metadata.getAccumulatorStateDescriptors();
        List<StateFieldAndDescriptor> stateFieldAndDescriptors = new ArrayList<>();
        for (int i = 0; i < stateDescriptors.size(); i++) {
            stateFieldAndDescriptors.add(new StateFieldAndDescriptor(
                    stateDescriptors.get(i),
                    definition.declareField(a(PRIVATE, FINAL), "stateSerializer_" + i, AccumulatorStateSerializer.class),
                    definition.declareField(a(PRIVATE, FINAL), "stateFactory_" + i, AccumulatorStateFactory.class),
                    definition.declareField(a(PRIVATE, FINAL), "state_" + i, AccumulatorState.class)));
        }
        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        int lambdaCount = metadata.getLambdaInterfaces().size();
        List<FieldDefinition> lambdaProviderFields = new ArrayList<>(lambdaCount);
        for (int i = 0; i < lambdaCount; i++) {
            lambdaProviderFields.add(definition.declareField(a(PRIVATE, FINAL), "lambdaProvider_" + i, LambdaProvider.class));
        }

        // Generate constructor
        generateWindowAccumulatorConstructor(
                definition,
                stateFieldAndDescriptors,
                lambdaProviderFields,
                callSiteBinder);
        generatePrivateConstructor(definition);

        // Generate methods
        generateCopy(definition, stateFieldAndDescriptors, lambdaProviderFields, WindowAccumulator.class);
        generateAddOrRemoveInputWindowIndex(
                definition,
                stateFields,
                argumentNullable,
                lambdaProviderFields,
                metadata.getInputFunction(),
                "addInput",
                callSiteBinder);
        metadata.getRemoveInputFunction().ifPresent(
                removeInputFunction -> generateAddOrRemoveInputWindowIndex(
                        definition,
                        stateFields,
                        argumentNullable,
                        lambdaProviderFields,
                        removeInputFunction,
                        "removeInput",
                        callSiteBinder));

        generateEvaluateFinal(definition, stateFields, metadata.getOutputFunction(), callSiteBinder);
        generateGetEstimatedSize(definition, stateFields);

        Class<? extends WindowAccumulator> windowAccumulatorClass = defineClass(definition, WindowAccumulator.class, callSiteBinder.getBindings(), classLoader);
        try {
            return windowAccumulatorClass.getConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void generateWindowAccumulatorConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<FieldDefinition> lambdaProviderFields,
            CallSiteBinder callSiteBinder)
    {
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, LambdaProvider.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        initializeStateFields(method, stateFieldAndDescriptors, callSiteBinder, false);
        initializeLambdaProviderFields(method, lambdaProviderFields, lambdaProviders);
        body.ret();
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
            List<Boolean> argumentNullable,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        Parameter arguments = arg("arguments", Page.class);
        parameters.add(arguments);
        Parameter mask = arg("mask", Optional.class);
        parameters.add(mask);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        List<Variable> parameterVariables = new ArrayList<>();
        for (int i = 0; i < argumentNullable.size(); i++) {
            parameterVariables.add(scope.declareVariable(Block.class, "block" + i));
        }
        Variable masksBlock = scope.declareVariable("masksBlock", body, mask.invoke("orElse", Object.class, constantNull(Object.class)).cast(Block.class));

        // Get all parameter blocks
        for (int i = 0; i < parameterVariables.size(); i++) {
            body.comment("%s = arguments.getBlock(%d);", parameterVariables.get(i).getName(), i)
                    .append(parameterVariables.get(i).set(arguments.invoke("getBlock", Block.class, constantInt(i))));
        }

        BytecodeBlock block = generateInputForLoop(
                arguments,
                stateField,
                argumentNullable,
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
            List<Boolean> argumentNullable,
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
                        argumentNullable.size(),
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
                                .condition(anyParametersAreNull(argumentNullable, index, channels, position))
                                .ifFalse(invokeInputFunction)))
                .ret();
    }

    private static BytecodeExpression anyParametersAreNull(
            List<Boolean> argumentNullable,
            Variable index,
            Variable channels,
            Variable position)
    {
        BytecodeExpression isNull = constantFalse();
        for (int inputChannel = 0; inputChannel < argumentNullable.size(); inputChannel++) {
            if (!argumentNullable.get(inputChannel)) {
                BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(inputChannel)).cast(int.class);
                isNull = BytecodeExpressions.or(isNull, index.invoke("isNull", boolean.class, getChannel, position));
            }
        }

        return isNull;
    }

    private static List<BytecodeExpression> getInvokeFunctionOnWindowIndexParameters(
            Scope scope,
            int inputParameterCount,
            List<FieldDefinition> lambdaProviderFields,
            List<FieldDefinition> stateField,
            Variable index,
            Variable channels,
            Variable position)
    {
        List<BytecodeExpression> expressions = new ArrayList<>();

        // state parameters
        for (FieldDefinition field : stateField) {
            expressions.add(scope.getThis().getField(field));
        }

        // input parameters
        for (int i = 0; i < inputParameterCount; i++) {
            BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(i)).cast(int.class);
            expressions.add(index.cast(InternalWindowIndex.class).invoke("getRawBlock", Block.class, getChannel, position));
        }

        // position parameter
        if (inputParameterCount > 0) {
            expressions.add(index.cast(InternalWindowIndex.class).invoke("getRawBlockPosition", int.class, position));
        }

        // lambda parameters
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            expressions.add(scope.getThis().getField(lambdaProviderField)
                    .invoke("getLambda", Object.class));
        }

        return expressions;
    }

    private static BytecodeBlock generateInputForLoop(
            Variable arguments,
            List<FieldDefinition> stateField,
            List<Boolean> argumentNullable,
            MethodHandle inputFunction,
            Scope scope,
            List<Variable> parameterVariables,
            List<FieldDefinition> lambdaProviderFields,
            Variable masksBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable positionVariable = scope.declareVariable(int.class, "position");
        Variable rowsVariable = scope.declareVariable(int.class, "rows");

        BytecodeBlock block = new BytecodeBlock()
                .append(arguments)
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);

        BytecodeNode loopBody = generateInvokeInputFunction(
                scope,
                stateField,
                positionVariable,
                parameterVariables,
                lambdaProviderFields,
                inputFunction,
                callSiteBinder,
                grouped);

        //  Wrap with null checks
        for (int parameterIndex = 0; parameterIndex < parameterVariables.size(); parameterIndex++) {
            if (!argumentNullable.get(parameterIndex)) {
                Variable variableDefinition = parameterVariables.get(parameterIndex);
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
            List<Variable> parameterVariables,
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

        List<BytecodeExpression> parameters = new ArrayList<>();

        // state parameters
        for (FieldDefinition field : stateField) {
            parameters.add(scope.getThis().getField(field));
        }

        // input parameters
        parameters.addAll(parameterVariables);

        // position parameter
        if (!parameterVariables.isEmpty()) {
            parameters.add(position);
        }

        // lambda parameters
        for (FieldDefinition lambdaProviderField : lambdaProviderFields) {
            parameters.add(scope.getThis().getField(lambdaProviderField)
                    .invoke("getLambda", Object.class));
        }

        block.append(invoke(callSiteBinder.bind(inputFunction), "input", parameters));
        return block;
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
            List<FieldDefinition> lambdaProviderFields,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, LambdaProvider.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        initializeStateFields(method, stateFieldAndDescriptors, callSiteBinder, grouped);
        initializeLambdaProviderFields(method, lambdaProviderFields, lambdaProviders);

        body.ret();
    }

    private static void initializeStateFields(
            MethodDefinition method,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        for (StateFieldAndDescriptor fieldAndDescriptor : stateFieldAndDescriptors) {
            AccumulatorStateDescriptor<?> accumulatorStateDescriptor = fieldAndDescriptor.getAccumulatorStateDescriptor();
            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateSerializerField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getSerializer(), AccumulatorStateSerializer.class)));
            body.append(thisVariable.setField(
                    fieldAndDescriptor.getStateFactoryField(),
                    loadConstant(callSiteBinder, accumulatorStateDescriptor.getFactory(), AccumulatorStateFactory.class)));

            // create the state object
            FieldDefinition stateField = fieldAndDescriptor.getStateField();
            BytecodeExpression stateFactory = thisVariable.getField(fieldAndDescriptor.getStateFactoryField());
            BytecodeExpression createStateInstance = stateFactory.invoke(grouped ? "createGroupedState" : "createSingleState", AccumulatorState.class);
            body.append(thisVariable.setField(stateField, createStateInstance.cast(stateField.getType())));
        }
    }

    private static void initializeLambdaProviderFields(MethodDefinition method, List<FieldDefinition> lambdaProviderFields, Parameter lambdaProviders)
    {
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        for (int i = 0; i < lambdaProviderFields.size(); i++) {
            body.append(thisVariable.setField(
                    lambdaProviderFields.get(i),
                    lambdaProviders.invoke("get", Object.class, constantInt(i))
                            .cast(LambdaProvider.class)));
        }
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
            Class<?> returnType)
    {
        MethodDefinition copy = definition.declareMethod(a(PUBLIC), "copy", type(returnType));
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
                .append(instanceCopy.ret());
    }

    private static AggregationMetadata normalizeAggregationMethods(AggregationMetadata metadata)
    {
        // change aggregations state variables to simply AccumulatorState to avoid any class loader issues in generated code
        int stateParameterCount = metadata.getAccumulatorStateDescriptors().size();
        int lambdaParameterCount = metadata.getLambdaInterfaces().size();
        return new AggregationMetadata(
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
