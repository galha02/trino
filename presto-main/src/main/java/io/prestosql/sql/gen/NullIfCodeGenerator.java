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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;

import java.util.List;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.gen.BytecodeUtils.ifWasNullPopAndGoto;

public class NullIfCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        Scope scope = generatorContext.getScope();

        RowExpression first = arguments.get(0);
        RowExpression second = arguments.get(1);

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        Variable firstValue = scope.createTempVariable(first.getType().getJavaType());
        BytecodeBlock block = new BytecodeBlock()
                .comment("check if first arg is null")
                .append(generatorContext.generate(first))
                .append(ifWasNullPopAndGoto(scope, notMatch, void.class))
                .dup(first.getType().getJavaType())
                .putVariable(firstValue);

        Type firstType = first.getType();
        Type secondType = second.getType();

        // if (equal(cast(first as <common type>), cast(second as <common type>))
        FunctionManager functionManager = generatorContext.getRegistry();
        FunctionHandle equalsHandle = functionManager.lookupOperator(EQUAL, fromTypes(firstType, secondType));
        FunctionMetadata equalsMetadata = functionManager.getFunctionMetadata(equalsHandle);
        ScalarFunctionImplementation equalsFunction = functionManager.getScalarFunctionImplementation(equalsHandle);
        BytecodeNode equalsCall = generatorContext.generateCall(
                EQUAL.name(),
                equalsFunction,
                ImmutableList.of(
                        cast(generatorContext, firstValue, firstType, equalsMetadata.getArgumentTypes().get(0)),
                        cast(generatorContext, generatorContext.generate(second), secondType, equalsMetadata.getArgumentTypes().get(1))));

        BytecodeBlock conditionBlock = new BytecodeBlock()
                .append(equalsCall)
                .append(BytecodeUtils.ifWasNullClearPopAndGoto(scope, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        BytecodeBlock trueBlock = new BytecodeBlock()
                .append(generatorContext.wasNull().set(constantTrue()))
                .pop(first.getType().getJavaType())
                .pushJavaDefault(first.getType().getJavaType());

        // else return first (which is still on the stack
        block.append(new IfStatement()
                .condition(conditionBlock)
                .ifTrue(trueBlock)
                .ifFalse(notMatch));

        return block;
    }

    private static BytecodeNode cast(
            BytecodeGeneratorContext generatorContext,
            BytecodeNode argument,
            Type actualType,
            Type requiredType)
    {
        if (actualType.equals(requiredType)) {
            return argument;
        }

        FunctionManager functionManager = generatorContext.getRegistry();
        FunctionHandle functionHandle = functionManager.lookupCast(actualType.getTypeSignature(), requiredType.getTypeSignature());

        // TODO: do we need a full function call? (nullability checks, etc)
        return generatorContext.generateCall(CAST.name(), functionManager.getScalarFunctionImplementation(functionHandle), ImmutableList.of(argument));
    }
}
