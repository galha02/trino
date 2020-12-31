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
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.type.TypeSignature;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class JsonStringToArrayCast
        extends SqlScalarFunction
{
    public static final JsonStringToArrayCast JSON_STRING_TO_ARRAY = new JsonStringToArrayCast();
    public static final String JSON_STRING_TO_ARRAY_NAME = "$internal$json_string_to_array_cast";

    private JsonStringToArrayCast()
    {
        super(new FunctionMetadata(
                new Signature(
                        JSON_STRING_TO_ARRAY_NAME,
                        ImmutableList.of(typeVariable("T")),
                        ImmutableList.of(),
                        arrayType(new TypeSignature("T")),
                        ImmutableList.of(VARCHAR.getTypeSignature()),
                        false),
                true,
                ImmutableList.of(new FunctionArgumentDefinition(false)),
                true,
                true,
                "",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        return JSON_TO_ARRAY.specialize(functionBinding);
    }
}
