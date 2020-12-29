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
package io.trino.operator.annotations;

import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.prestosql.spi.function.CastDependency;
import io.prestosql.spi.function.Convention;
import io.prestosql.spi.function.FunctionDependency;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.tree.QualifiedName;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.annotations.FunctionsParserHelper.containsImplementationDependencyAnnotation;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;

public interface ImplementationDependency
{
    void declareDependencies(FunctionDependencyDeclarationBuilder builder);

    Object resolve(FunctionBinding functionBinding, FunctionDependencies functionDependencies);

    static boolean isImplementationDependencyAnnotation(Annotation annotation)
    {
        return annotation instanceof TypeParameter ||
                annotation instanceof LiteralParameter ||
                annotation instanceof FunctionDependency ||
                annotation instanceof OperatorDependency ||
                annotation instanceof CastDependency;
    }

    static Optional<Annotation> getImplementationDependencyAnnotation(AnnotatedElement element)
    {
        if (!containsImplementationDependencyAnnotation(element.getAnnotations())) {
            return Optional.empty();
        }
        Annotation[] annotations = element.getAnnotations();
        checkArgument(annotations.length == 1, "Meta parameters may only have a single annotation [%s]", element);
        return Optional.of(annotations[0]);
    }

    static void validateImplementationDependencyAnnotation(AnnotatedElement element, Annotation annotation, Set<String> typeParametersNames, Collection<String> literalParameters)
    {
        if (annotation instanceof TypeParameter) {
            checkTypeParameters(parseTypeSignature(((TypeParameter) annotation).value(), ImmutableSet.of()), typeParametersNames, element);
        }
        if (annotation instanceof LiteralParameter) {
            checkArgument(literalParameters.contains(((LiteralParameter) annotation).value()), "Parameter injected by @LiteralParameter must be declared with @LiteralParameters on the method [%s]", element);
        }
    }

    static void checkTypeParameters(TypeSignature typeSignature, Set<String> typeParameterNames, AnnotatedElement element)
    {
        // Check recursively if `typeSignature` contains something like `T(bigint)`
        if (typeParameterNames.contains(typeSignature.getBase())) {
            checkArgument(typeSignature.getParameters().isEmpty(), "Expected type parameter not to take parameters, but got '%s' on method [%s]", typeSignature.getBase(), element);
            return;
        }

        for (TypeSignatureParameter parameter : typeSignature.getParameters()) {
            Optional<TypeSignature> childTypeSignature = parameter.getTypeSignatureOrNamedTypeSignature();
            if (childTypeSignature.isPresent()) {
                checkTypeParameters(childTypeSignature.get(), typeParameterNames, element);
            }
        }
    }

    final class Factory
    {
        private Factory() {}

        public static ImplementationDependency createDependency(Annotation annotation, Set<String> literalParameters, Class<?> type)
        {
            if (annotation instanceof TypeParameter) {
                return new TypeImplementationDependency(((TypeParameter) annotation).value());
            }
            if (annotation instanceof LiteralParameter) {
                return new LiteralImplementationDependency(((LiteralParameter) annotation).value());
            }

            if (annotation instanceof FunctionDependency) {
                FunctionDependency functionDependency = (FunctionDependency) annotation;
                return new FunctionImplementationDependency(
                        QualifiedName.of(functionDependency.name()),
                        Arrays.stream(functionDependency.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()),
                        toInvocationConvention(functionDependency.convention()),
                        type);
            }
            if (annotation instanceof OperatorDependency) {
                OperatorDependency operatorDependency = (OperatorDependency) annotation;
                OperatorType operator = operatorDependency.operator();
                checkArgument(operator != OperatorType.CAST && operator != OperatorType.SATURATED_FLOOR_CAST, "%s not supported for OperatorDependency", operator);
                return new OperatorImplementationDependency(
                        operator,
                        Arrays.stream(operatorDependency.argumentTypes())
                                .map(signature -> parseTypeSignature(signature, literalParameters))
                                .collect(toImmutableList()),
                        toInvocationConvention(operatorDependency.convention()),
                        type);
            }
            if (annotation instanceof CastDependency) {
                CastDependency castDependency = (CastDependency) annotation;
                return new CastImplementationDependency(
                        parseTypeSignature(castDependency.fromType(), literalParameters),
                        parseTypeSignature(castDependency.toType(), literalParameters),
                        toInvocationConvention(castDependency.convention()),
                        type);
            }

            throw new IllegalArgumentException("Unsupported annotation " + annotation.getClass().getSimpleName());
        }

        private static InvocationConvention toInvocationConvention(Convention convention)
        {
            List<InvocationConvention.InvocationArgumentConvention> argumentConventions = new ArrayList<>();
            Collections.addAll(argumentConventions, convention.arguments());
            return new InvocationConvention(argumentConventions, convention.result(), convention.session(), false);
        }
    }
}
