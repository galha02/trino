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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class Signature
{
    private static final String OPERATOR_PREFIX = "$operator$";

    private final String name;
    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<LongVariableConstraint> longVariableConstraints;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    private Signature(
            String name,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            boolean variableArity)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeVariableConstraints, "typeVariableConstraints is null");
        requireNonNull(longVariableConstraints, "longVariableConstraints is null");

        this.name = name;
        this.typeVariableConstraints = ImmutableList.copyOf(typeVariableConstraints);
        this.longVariableConstraints = ImmutableList.copyOf(longVariableConstraints);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
    }

    public static boolean isOperatorName(String mangledName)
    {
        return mangledName.startsWith(OPERATOR_PREFIX);
    }

    public static String mangleOperatorName(OperatorType operatorType)
    {
        return OPERATOR_PREFIX + operatorType.name();
    }

    @VisibleForTesting
    public static OperatorType unmangleOperator(String mangledName)
    {
        checkArgument(mangledName.startsWith(OPERATOR_PREFIX), "not a mangled operator name: %s", mangledName);
        return OperatorType.valueOf(mangledName.substring(OPERATOR_PREFIX.length()).toUpperCase(Locale.ENGLISH));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public List<TypeVariableConstraint> getTypeVariableConstraints()
    {
        return typeVariableConstraints;
    }

    @JsonProperty
    public List<LongVariableConstraint> getLongVariableConstraints()
    {
        return longVariableConstraints;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name.toLowerCase(Locale.US), typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Signature)) {
            return false;
        }
        Signature other = (Signature) obj;
        return name.equalsIgnoreCase(other.name) &&
                Objects.equals(this.typeVariableConstraints, other.typeVariableConstraints) &&
                Objects.equals(this.longVariableConstraints, other.longVariableConstraints) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity);
    }

    @Override
    public String toString()
    {
        List<String> allConstraints = concat(
                typeVariableConstraints.stream().map(TypeVariableConstraint::toString),
                longVariableConstraints.stream().map(LongVariableConstraint::toString))
                .collect(Collectors.toList());

        return name + (allConstraints.isEmpty() ? "" : "<" + Joiner.on(",").join(allConstraints) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    public Signature withName(String name)
    {
        return fromJson(
                name,
                typeVariableConstraints,
                longVariableConstraints,
                returnType,
                argumentTypes,
                variableArity);
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeVariableConstraint withVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, false, variadicBound, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, true, false, variadicBound, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint typeVariable(String name)
    {
        return new TypeVariableConstraint(name, false, false, null, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint comparableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, true, false, null, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint orderableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, true, variadicBound, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint orderableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, false, true, null, ImmutableSet.of(), ImmutableSet.of());
    }

    public static TypeVariableConstraint castableToTypeParameter(String name, TypeSignature... toType)
    {
        return new TypeVariableConstraint(name, false, false, null, ImmutableSet.copyOf(toType), ImmutableSet.of());
    }

    public static TypeVariableConstraint castableToTypeParameter(String name, Set<TypeSignature> toType)
    {
        return new TypeVariableConstraint(name, false, false, null, toType, ImmutableSet.of());
    }

    public static TypeVariableConstraint castableFromTypeParameter(String name, TypeSignature... toType)
    {
        return new TypeVariableConstraint(name, false, false, null, ImmutableSet.of(), ImmutableSet.copyOf(toType));
    }

    public static TypeVariableConstraint castableFromTypeParameter(String name, Set<TypeSignature> toType)
    {
        return new TypeVariableConstraint(name, false, false, null, ImmutableSet.of(), toType);
    }

    public static LongVariableConstraint longVariableExpression(String variable, String expression)
    {
        return new LongVariableConstraint(variable, expression);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private final List<TypeVariableConstraint> typeVariableConstraints = new ArrayList<>();
        private final List<LongVariableConstraint> longVariableConstraints = new ArrayList<>();
        private TypeSignature returnType;
        private final List<TypeSignature> argumentTypes = new ArrayList<>();
        private boolean variableArity;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder operatorType(OperatorType operatorType)
        {
            this.name = mangleOperatorName(requireNonNull(operatorType, "operatorType is null"));
            return this;
        }

        public Builder typeVariable(String name)
        {
            typeVariableConstraints.add(Signature.typeVariable(name));
            return this;
        }

        public Builder comparableTypeParameter(String name)
        {
            typeVariableConstraints.add(Signature.comparableTypeParameter(name));
            return this;
        }

        public Builder orderableTypeParameter(String name)
        {
            typeVariableConstraints.add(Signature.orderableTypeParameter(name));
            return this;
        }

        public Builder castableToTypeParameter(String name, TypeSignature... toType)
        {
            typeVariableConstraints.add(Signature.castableToTypeParameter(name, toType));
            return this;
        }

        public Builder castableFromTypeParameter(String name, TypeSignature... toType)
        {
            typeVariableConstraints.add(Signature.castableFromTypeParameter(name, toType));
            return this;
        }

        public Builder variadicTypeParameter(String name, String variadicBound)
        {
            typeVariableConstraints.add(withVariadicBound(name, variadicBound));
            return this;
        }

        public Builder typeVariableConstraint(TypeVariableConstraint typeVariableConstraint)
        {
            this.typeVariableConstraints.add(requireNonNull(typeVariableConstraint, "typeVariableConstraint is null"));
            return this;
        }

        public Builder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints)
        {
            this.typeVariableConstraints.addAll(requireNonNull(typeVariableConstraints, "typeVariableConstraints is null"));
            return this;
        }

        public Builder returnType(Type returnType)
        {
            return returnType(returnType.getTypeSignature());
        }

        public Builder returnType(TypeSignature returnType)
        {
            this.returnType = requireNonNull(returnType, "returnType is null");
            return this;
        }

        public Builder longVariable(String name, String expression)
        {
            this.longVariableConstraints.add(new LongVariableConstraint(name, expression));
            return this;
        }

        public Builder argumentType(Type type)
        {
            return argumentType(requireNonNull(type, "type is null").getTypeSignature());
        }

        public Builder argumentType(TypeSignature type)
        {
            argumentTypes.add(requireNonNull(type, "type is null"));
            return this;
        }

        public Builder argumentTypes(List<TypeSignature> argumentTypes)
        {
            this.argumentTypes.addAll(requireNonNull(argumentTypes, "argumentTypes is null"));
            return this;
        }

        public Builder variableArity()
        {
            this.variableArity = true;
            return this;
        }

        public Signature build()
        {
            return fromJson(name, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
        }
    }

    /**
     * This method is only visible for JSON deserialization.
     * @deprecated use builder
     */
    @Deprecated
    @JsonCreator
    public static Signature fromJson(
            @JsonProperty("name") String name,
            @JsonProperty("typeVariableConstraints") List<TypeVariableConstraint> typeVariableConstraints,
            @JsonProperty("longVariableConstraints") List<LongVariableConstraint> longVariableConstraints,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        return new Signature(name, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
    }
}
