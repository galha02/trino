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
package io.prestosql.tests.functions;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tests.TestGroups.JSON_FUNCTIONS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;

public class TestFunctions
        extends ProductTest
{
    @Test(groups = JSON_FUNCTIONS)
    public void testScalarFunction()
    {
        assertThat(query("SELECT upper('value')")).containsExactly(row("VALUE"));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testAggregate()
    {
        assertThat(query("SELECT min(x) FROM (VALUES 1,2,3,4) t(x)")).containsExactly(row(1));
    }
}
