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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.AggregationFunctionMetadata.AggregationFunctionMetadataBuilder;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SignatureBinder;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.aggregation.AggregationFromAnnotationsParser.AccumulatorStateDetails;
import io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.InOutStateSerializer;
import io.trino.operator.annotations.ImplementationDependency;
import io.trino.spi.TrinoException;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.InOut;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.ParametricFunctionHelpers.bindDependencies;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
import static io.trino.operator.aggregation.state.StateCompiler.generateInOutStateFactory;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateFactory;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static io.trino.operator.aggregation.state.StateCompiler.getSerializedType;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricAggregation
        extends SqlAggregationFunction
{
    private final ParametricImplementationsGroup<AggregationImplementation> implementations;
    private final AccumulatorStateDetails stateDetails;

    public ParametricAggregation(
            Signature signature,
            AggregationHeader details,
            AccumulatorStateDetails stateDetails,
            ParametricImplementationsGroup<AggregationImplementation> implementations)
    {
        super(
                createFunctionMetadata(signature, details, implementations.getFunctionNullability()),
                createAggregationFunctionMetadata(details, stateDetails));
        this.stateDetails = requireNonNull(stateDetails, "stateDetails is null");
        checkArgument(implementations.getFunctionNullability().isReturnNullable(), "currently aggregates are required to be nullable");
        this.implementations = requireNonNull(implementations, "implementations is null");
    }

    private static FunctionMetadata createFunctionMetadata(Signature signature, AggregationHeader details, FunctionNullability functionNullability)
    {
        FunctionMetadata.Builder functionMetadata = FunctionMetadata.aggregateBuilder()
                .signature(signature)
                .canonicalName(details.getName());

        if (details.getDescription().isPresent()) {
            functionMetadata.description(details.getDescription().get());
        }
        else {
            functionMetadata.noDescription();
        }

        if (details.isHidden()) {
            functionMetadata.hidden();
        }
        if (details.isDeprecated()) {
            functionMetadata.deprecated();
        }

        if (functionNullability.isReturnNullable()) {
            functionMetadata.nullable();
        }
        functionMetadata.argumentNullability(functionNullability.getArgumentNullable());

        return functionMetadata.build();
    }

    private static AggregationFunctionMetadata createAggregationFunctionMetadata(AggregationHeader details, AccumulatorStateDetails stateDetails)
    {
        AggregationFunctionMetadataBuilder builder = AggregationFunctionMetadata.builder();
        if (details.isOrderSensitive()) {
            builder.orderSensitive();
        }
        if (details.isDecomposable()) {
            builder.intermediateType(getSerializedType(stateDetails));
        }
        return builder.build();
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        declareDependencies(builder, implementations.getExactImplementations().values());
        declareDependencies(builder, implementations.getSpecializedImplementations());
        declareDependencies(builder, implementations.getGenericImplementations());
        return builder.build();
    }

    private static void declareDependencies(FunctionDependencyDeclarationBuilder builder, Collection<AggregationImplementation> implementations)
    {
        for (AggregationImplementation implementation : implementations) {
            for (ImplementationDependency dependency : implementation.getInputDependencies()) {
                dependency.declareDependencies(builder);
            }
            for (ImplementationDependency dependency : implementation.getCombineDependencies()) {
                dependency.declareDependencies(builder);
            }
            for (ImplementationDependency dependency : implementation.getOutputDependencies()) {
                dependency.declareDependencies(builder);
            }
        }
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        // Find implementation matching arguments
        AggregationImplementation concreteImplementation = findMatchingImplementation(boundSignature);

        // Build state factory and serializer
        AccumulatorStateDescriptor<?> accumulatorStateDescriptor = generateAccumulatorStateDescriptor(getFunctionMetadata().getSignature(), boundSignature, stateDetails);

        // Bind provided dependencies to aggregation method handlers
        FunctionMetadata metadata = getFunctionMetadata();
        FunctionBinding functionBinding = SignatureBinder.bindFunction(metadata.getFunctionId(), metadata.getSignature(), boundSignature);
        MethodHandle inputHandle = bindDependencies(concreteImplementation.getInputFunction(), concreteImplementation.getInputDependencies(), functionBinding, functionDependencies);
        Optional<MethodHandle> removeInputHandle = concreteImplementation.getRemoveInputFunction().map(
                removeInputFunction -> bindDependencies(removeInputFunction, concreteImplementation.getRemoveInputDependencies(), functionBinding, functionDependencies));

        Optional<MethodHandle> combineHandle = concreteImplementation.getCombineFunction();
        if (getAggregationMetadata().isDecomposable()) {
            checkArgument(combineHandle.isPresent(), "Decomposable method %s does not have a combine method", boundSignature.getName());
            combineHandle = combineHandle.map(combineFunction -> bindDependencies(combineFunction, concreteImplementation.getCombineDependencies(), functionBinding, functionDependencies));
        }
        else {
            checkArgument(concreteImplementation.getCombineFunction().isEmpty(), "Decomposable method %s does not have a combine method", boundSignature.getName());
        }

        MethodHandle outputHandle = bindDependencies(concreteImplementation.getOutputFunction(), concreteImplementation.getOutputDependencies(), functionBinding, functionDependencies);

        List<AggregationParameterKind> inputParameterKinds = concreteImplementation.getInputParameterKinds();
        inputHandle = normalizeInputMethod(inputHandle, boundSignature, inputParameterKinds);
        removeInputHandle = removeInputHandle.map(function -> normalizeInputMethod(function, boundSignature, inputParameterKinds));

        return new AggregationMetadata(
                inputHandle,
                removeInputHandle,
                combineHandle,
                outputHandle,
                ImmutableList.of(accumulatorStateDescriptor));
    }

    private static AccumulatorStateDescriptor<?> generateAccumulatorStateDescriptor(Signature signature, BoundSignature boundSignature, AccumulatorStateDetails stateDetails)
    {
        if (stateDetails.getStateClass().equals(InOut.class)) {
            return createInOutAccumulatorStateDescriptor(signature, boundSignature, stateDetails);
        }
        return generateAccumulatorStateDescriptor(stateDetails.getStateClass());
    }

    private static AccumulatorStateDescriptor<InOut> createInOutAccumulatorStateDescriptor(Signature signature, BoundSignature boundSignature, AccumulatorStateDetails stateDetails)
    {
        Type type = extractInOutType(signature, boundSignature, stateDetails);
        InOutStateSerializer inOutStateSerializer = new InOutStateSerializer(type);
        AccumulatorStateFactory<InOut> inOutAccumulatorStateFactory = generateInOutStateFactory(type);
        return new AccumulatorStateDescriptor<>(
                InOut.class,
                inOutStateSerializer,
                inOutAccumulatorStateFactory);
    }

    private static Type extractInOutType(Signature signature, BoundSignature boundSignature, AccumulatorStateDetails stateDetails)
    {
        TypeSignature inOutType = stateDetails.getStateType().orElseThrow();
        if (signature.getReturnType().equals(inOutType)) {
            return boundSignature.getReturnType();
        }
        List<TypeSignature> declaredArgumentTypes = signature.getArgumentTypes();
        List<Type> actualArgumentTypes = boundSignature.getArgumentTypes();
        for (int i = 0; i < declaredArgumentTypes.size(); i++) {
            TypeSignature argumentType = declaredArgumentTypes.get(i);
            if (argumentType.equals(inOutType)) {
                return actualArgumentTypes.get(i);
            }
        }
        throw new IllegalArgumentException(format("Could not determine type %s from function signature %s", inOutType, signature));
    }

    private static <T extends AccumulatorState> AccumulatorStateDescriptor<T> generateAccumulatorStateDescriptor(Class<T> stateClass)
    {
        return new AccumulatorStateDescriptor<>(
                stateClass,
                generateStateSerializer(stateClass),
                generateStateFactory(stateClass));
    }

    public Class<?> getStateClass()
    {
        return stateDetails.getStateClass();
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<AggregationImplementation> getImplementations()
    {
        return implementations;
    }

    private AggregationImplementation findMatchingImplementation(BoundSignature boundSignature)
    {
        Signature signature = boundSignature.toSignature();
        Optional<AggregationImplementation> foundImplementation = Optional.empty();
        if (implementations.getExactImplementations().containsKey(signature)) {
            foundImplementation = Optional.of(implementations.getExactImplementations().get(signature));
        }
        else {
            for (AggregationImplementation candidate : implementations.getGenericImplementations()) {
                if (candidate.areTypesAssignable(boundSignature)) {
                    if (foundImplementation.isPresent()) {
                        throw new TrinoException(AMBIGUOUS_FUNCTION_CALL, format("Ambiguous function call (%s) for %s", boundSignature, getFunctionMetadata().getSignature()));
                    }
                    foundImplementation = Optional.of(candidate);
                }
            }
        }

        if (foundImplementation.isEmpty()) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", boundSignature, getFunctionMetadata().getSignature()));
        }
        return foundImplementation.get();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ParametricAggregation.class.getSimpleName() + "[", "]")
                .add("signature=" + implementations.getSignature())
                .toString();
    }
}
