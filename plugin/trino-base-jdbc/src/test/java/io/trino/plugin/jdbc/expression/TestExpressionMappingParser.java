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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestExpressionMappingParser
{
    @Test
    public void testCapture()
    {
        assertExpressionPattern(
                "b: bigint",
                new ExpressionCapture(
                        "b",
                        type("bigint")));

        assertExpressionPattern(
                "bar: varchar(n)",
                new ExpressionCapture(
                        "bar",
                        type("varchar", parameter("n"))));
    }

    @Test
    public void testParameterizedType()
    {
        assertExpressionPattern(
                "bar: varchar(3)",
                new ExpressionCapture(
                        "bar",
                        type("varchar", parameter(3L))));
    }

    @Test
    public void testCallPattern()
    {
        assertExpressionPattern(
                "$like_pattern(a: varchar(n), b: varchar(m))",
                new CallPattern(
                        "$like_pattern",
                        List.of(
                                new ExpressionCapture("a", type("varchar", parameter("n"))),
                                new ExpressionCapture("b", type("varchar", parameter("m")))),
                        Optional.empty()));

        assertExpressionPattern(
                "$like_pattern(a: varchar(n), b: varchar(m)): boolean",
                new CallPattern(
                        "$like_pattern",
                        List.of(
                                new ExpressionCapture("a", type("varchar", parameter("n"))),
                                new ExpressionCapture("b", type("varchar", parameter("m")))),
                        Optional.of(type("boolean"))));
    }

    private static void assertExpressionPattern(String expressionPattern, ExpressionPattern expected)
    {
        assertExpressionPattern(expressionPattern, expressionPattern, expected);
    }

    private static void assertExpressionPattern(String expressionPattern, String canonical, ExpressionPattern expected)
    {
        assertEquals(expressionPattern(expressionPattern), expected);
        assertEquals(expected.toString(), canonical);
    }

    private static ExpressionPattern expressionPattern(String expressionPattern)
    {
        return new ExpressionMappingParser().createExpressionPattern(expressionPattern);
    }

    private static TypePattern type(String baseName)
    {
        return new TypePattern(baseName, ImmutableList.of());
    }

    private static TypePattern type(String baseName, TypeParameterPattern... parameter)
    {
        return new TypePattern(baseName, ImmutableList.copyOf(parameter));
    }

    private static TypeParameterPattern parameter(long value)
    {
        return new LongTypeParameter(value);
    }

    private static TypeParameterPattern parameter(String name)
    {
        return new TypeParameterCapture(name);
    }
}
