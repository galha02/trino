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
package io.trino.plugin.deltalake.kernel.clients;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.expressions.PredicateEvaluator;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

public class KernelExpressionHandler
        implements ExpressionHandler
{
    @Override
    public ExpressionEvaluator getEvaluator(StructType structType, Expression expression, DataType dataType)
    {
        return null;
    }

    @Override
    public PredicateEvaluator getPredicateEvaluator(StructType structType, Predicate predicate)
    {
        return null;
    }

    @Override
    public ColumnVector createSelectionVector(boolean[] booleans, int i, int i1)
    {
        return null;
    }
}
