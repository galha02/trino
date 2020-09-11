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
package io.prestosql.operator.scalar;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

public class TestCustomFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setupClass()
    {
        registerScalar(CustomFunctions.class);
    }

    @Test
    public void testCustomAdd()
    {
        assertFunction("custom_add(123, 456)", BIGINT, 579L);
    }

    @Test
    public void testSliceIsNull()
    {
        assertFunction("custom_is_null(CAST(NULL AS VARCHAR))", BOOLEAN, true);
        assertFunction("custom_is_null('not null')", BOOLEAN, false);
    }

    @Test
    public void testLongIsNull()
    {
        assertFunction("custom_is_null(CAST(NULL AS BIGINT))", BOOLEAN, true);
        assertFunction("custom_is_null(0)", BOOLEAN, false);
    }

    @Test
    public void testIdentityFunction()
    {
        assertFunction("\"identity&function\"(\"identity.function\"(123))", BIGINT, 123L);
        assertFunction("\"identity.function\"(\"identity&function\"(123))", BIGINT, 123L);
    }
}
