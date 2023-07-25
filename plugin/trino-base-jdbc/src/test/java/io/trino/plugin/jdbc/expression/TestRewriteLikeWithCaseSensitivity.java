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
package io.trino.plugin.jdbc.expression;

import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteLikeWithCaseSensitivity
        extends BaseTestRewriteLikeWithCaseSensitivity
{
    private final RewriteLikeWithCaseSensitivity rewrite = new RewriteLikeWithCaseSensitivity();

    @Override
    protected ConnectorExpressionRule<Call, ParameterizedExpression> getRewrite()
    {
        return rewrite;
    }

    @Test
    public void testRewriteLikeEscapeCallInvalidNumberOfArguments()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(new Variable("case_sensitive_value", VARCHAR)));

        assertNoRewrite(expression);
    }

    @Test
    public void testRewriteLikeCallInvalidNumberOfArguments()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(new Variable("case_sensitive_value", VARCHAR)));

        assertNoRewrite(expression);
    }

    @Test
    public void testRewriteLikeCallInvalidTypeValue()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", BIGINT),
                        new Variable("pattern", VARCHAR)));

        assertNoRewrite(expression);
    }

    @Test
    public void testRewriteLikeCallInvalidTypePattern()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", BIGINT)));

        assertNoRewrite(expression);
    }

    @Test
    public void testRewriteLikeCallOnCaseInsensitiveValue()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_insensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR)));

        assertNoRewrite(expression);
    }

    @Test
    public void testRewriteLikeCallOnCaseSensitiveValue()
    {
        Call expression = new Call(
                BOOLEAN,
                new FunctionName("$like"),
                List.of(
                        new Variable("case_sensitive_value", VARCHAR),
                        new Variable("pattern", VARCHAR)));

        ParameterizedExpression rewritten = apply(expression).orElseThrow();
        assertThat(rewritten.expression()).isEqualTo("\"case_sensitive_value\" LIKE \"pattern\"");
        assertThat(rewritten.parameters()).isEqualTo(List.of(
                new QueryParameter(VARCHAR, Optional.of("case_sensitive_value")),
                new QueryParameter(VARCHAR, Optional.of("pattern"))));
    }
}
