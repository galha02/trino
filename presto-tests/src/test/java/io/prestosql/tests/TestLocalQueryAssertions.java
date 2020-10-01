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

import io.prestosql.Session;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.QueryRunner;

import static io.airlift.testing.Closeables.closeAllSuppress;
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
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(createSession()).build();

        try {
            configureCatalog(queryRunner);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Override
    public void testIsCorrectlyPushedDown()
    {
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation")).isCorrectlyPushedDown())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("isCorrectlyPushedDown() currently does not work with LocalQueryRunner");
    }

    @Override
    public void testIsCorrectlyPushedDownWithSession()
    {
        Session baseSession = Session.builder(getSession())
                .setCatalog("jdbc_with_aggregation_pushdown_disabled")
                .build();

        assertThatThrownBy(() -> assertThat(query(baseSession, "SELECT name FROM nation")).isCorrectlyPushedDown())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("isCorrectlyPushedDown() currently does not work with LocalQueryRunner");
    }
}
