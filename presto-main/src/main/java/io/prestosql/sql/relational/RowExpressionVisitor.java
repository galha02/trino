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
package io.prestosql.sql.relational;

public interface RowExpressionVisitor<R, C>
{
    R visitCall(CallExpression call, C context);

    R visitSpecialForm(SpecialForm specialForm, C context);

    R visitInputReference(InputReferenceExpression reference, C context);

    R visitConstant(ConstantExpression literal, C context);

    R visitLambda(LambdaDefinitionExpression lambda, C context);

    R visitVariableReference(VariableReferenceExpression reference, C context);
}
