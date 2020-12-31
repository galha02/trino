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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MoreCollectors;
import io.trino.metadata.Signature;
import io.trino.operator.ParametricImplementationsGroup;
import io.trino.operator.annotations.FunctionsParserHelper;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.type.TypeSignature;

import javax.annotation.Nullable;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.aggregation.AggregationImplementation.Parser.parseImplementation;
import static io.trino.operator.annotations.FunctionsParserHelper.parseDescription;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AggregationFromAnnotationsParser
{
    private AggregationFromAnnotationsParser() {}

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    @VisibleForTesting
    public static ParametricAggregation parseFunctionDefinitionWithTypesConstraint(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (ParametricAggregation aggregation : parseFunctionDefinitions(clazz)) {
            Signature signature = aggregation.getFunctionMetadata().getSignature();
            if (signature.getReturnType().equals(returnType) &&
                    signature.getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<ParametricAggregation> parseFunctionDefinitions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");
        boolean deprecated = aggregationDefinition.getAnnotationsByType(Deprecated.class).length > 0;

        ImmutableList.Builder<ParametricAggregation> builder = ImmutableList.builder();

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    Optional<Method> removeInputFunction = getRemoveInputFunction(aggregationDefinition, inputFunction);
                    for (AggregationHeader header : parseHeaders(aggregationDefinition, outputFunction)) {
                        AggregationImplementation onlyImplementation = parseImplementation(aggregationDefinition, header, stateClass, inputFunction, removeInputFunction, outputFunction, combineFunction);
                        ParametricImplementationsGroup<AggregationImplementation> implementations = ParametricImplementationsGroup.of(onlyImplementation);
                        builder.add(new ParametricAggregation(implementations.getSignature(), header, implementations, deprecated));
                    }
                }
            }
        }

        return builder.build();
    }

    public static ParametricAggregation parseFunctionDefinition(Class<?> aggregationDefinition)
    {
        ParametricImplementationsGroup.Builder<AggregationImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
        AggregationHeader header = parseHeader(aggregationDefinition);
        boolean deprecated = aggregationDefinition.getAnnotationsByType(Deprecated.class).length > 0;

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            Method outputFunction = getOnlyElement(getOutputFunctions(aggregationDefinition, stateClass));
            for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                Optional<Method> removeInputFunction = getRemoveInputFunction(aggregationDefinition, inputFunction);
                AggregationImplementation implementation = parseImplementation(aggregationDefinition, header, stateClass, inputFunction, removeInputFunction, outputFunction, combineFunction);
                implementationsBuilder.addImplementation(implementation);
            }
        }

        ParametricImplementationsGroup<AggregationImplementation> implementations = implementationsBuilder.build();
        return new ParametricAggregation(implementations.getSignature(), header, implementations, deprecated);
    }

    private static AggregationHeader parseHeader(AnnotatedElement aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");
        return new AggregationHeader(
                aggregationAnnotation.value(),
                parseDescription(aggregationDefinition),
                aggregationAnnotation.decomposable(),
                aggregationAnnotation.isOrderSensitive(),
                aggregationAnnotation.hidden());
    }

    private static List<AggregationHeader> parseHeaders(AnnotatedElement aggregationDefinition, AnnotatedElement toParse)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);

        return getNames(toParse, aggregationAnnotation).stream()
                .map(name ->
                        new AggregationHeader(
                                name,
                                parseDescription(aggregationDefinition, toParse),
                                aggregationAnnotation.decomposable(),
                                aggregationAnnotation.isOrderSensitive(),
                                aggregationAnnotation.hidden()))
                .collect(toImmutableList());
    }

    private static List<String> getNames(@Nullable AnnotatedElement outputFunction, AggregationFunction aggregationAnnotation)
    {
        List<String> defaultNames = ImmutableList.<String>builder().add(aggregationAnnotation.value()).addAll(Arrays.asList(aggregationAnnotation.alias())).build();

        if (outputFunction == null) {
            return defaultNames;
        }

        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation == null) {
            return defaultNames;
        }
        else {
            return ImmutableList.<String>builder().add(annotation.value()).addAll(Arrays.asList(annotation.alias())).build();
        }
    }

    private static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> combineFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 0)] == stateClass)
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 1)] == stateClass)
                .collect(toImmutableList());

        checkArgument(combineFunctions.size() == 1, "There must be exactly one @CombineFunction in class %s for the @AggregationState %s", clazz.toGenericString(), stateClass.toGenericString());
        return getOnlyElement(combineFunctions);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> outputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass)
                .collect(toImmutableList());

        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> (method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass))
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static Optional<Method> getRemoveInputFunction(Class<?> clazz, Method inputFunction)
    {
        // Only include methods which take the same parameters as the corresponding input function
        return FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, RemoveInputFunction.class).stream()
                .filter(method -> Arrays.equals(method.getParameterTypes(), inputFunction.getParameterTypes()))
                .filter(method -> Arrays.deepEquals(method.getParameterAnnotations(), inputFunction.getParameterAnnotations()))
                .collect(MoreCollectors.toOptional());
    }

    private static Set<Class<?>> getStateClasses(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = AggregationImplementation.Parser.findAggregationStateParamType(inputFunction);

            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
    }
}
