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
package io.trino.plugin.pinot.query.predicate;

import io.airlift.slice.Slice;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.List;

public class PinotEqualsNotEqualsPredicate
        implements PinotPredicate
{
    private final String columnName;
    private final String value;
    private final String operator;
    private final boolean valueIsString;

    public static boolean supportsCall(Call call)
    {
        if (!new FunctionName("$equal").equals(call.getFunctionName()) &&
                !new FunctionName("$not_equal").equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (!(arguments.get(0) instanceof Variable && arguments.get(1) instanceof Constant)) {
            return false;
        }

        return true;
    }

    public PinotEqualsNotEqualsPredicate(Call call)
    {
        operator = new FunctionName("$equal").equals(call.getFunctionName()) ? "=" : "!=";
        List<ConnectorExpression> arguments = call.getArguments();
        columnName = ((Variable) arguments.get(0)).getName();
        Constant constant = (Constant) arguments.get(1);
        if (constant.getType() instanceof VarcharType) {
            valueIsString = true;
            value = ((Slice) constant.getValue()).toStringUtf8();
        }
        else {
            valueIsString = false;
            value = constant.getValue().toString();
        }
    }

    @Override
    public String toPQL()
    {
        String escape = valueIsString ? "'" : "";
        return String.format("%s %s %s%s%s",
                columnName, operator, escape, value, escape);
    }
}
