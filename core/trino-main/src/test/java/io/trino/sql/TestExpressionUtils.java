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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionUtils
{
    @Test
    public void testAnd()
    {
        Expression a = new SymbolReference(BOOLEAN, "a");
        Expression b = new SymbolReference(BOOLEAN, "b");
        Expression c = new SymbolReference(BOOLEAN, "c");
        Expression d = new SymbolReference(BOOLEAN, "d");
        Expression e = new SymbolReference(BOOLEAN, "e");

        assertThat(and(a, b, c, d, e)).isEqualTo(new LogicalExpression(AND, ImmutableList.of(a, b, c, d, e)));

        assertThat(combineConjuncts(a, b, a, c, d, c, e)).isEqualTo(new LogicalExpression(AND, ImmutableList.of(a, b, c, d, e)));
    }
}
