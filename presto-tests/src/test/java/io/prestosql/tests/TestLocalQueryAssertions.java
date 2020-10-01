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
package io.prestosql.tests;

import io.prestosql.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link io.prestosql.sql.query.QueryAssertions} with {@link io.prestosql.testing.LocalQueryRunner}
 */
public class TestLocalQueryAssertions
        extends AbstractQueryAssertionsTest
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return TestLocalQueries.createLocalQueryRunner();
    }

    @Override
    public void testIsCorrectlyPushedDown()
    {
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation")).isCorrectlyPushedDown())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("isCorrectlyPushedDown() currently does not work with LocalQueryRunner");
    }
}
