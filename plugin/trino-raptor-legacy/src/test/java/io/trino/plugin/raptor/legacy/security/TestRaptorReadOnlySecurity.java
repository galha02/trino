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
package io.trino.plugin.raptor.legacy.security;

import io.trino.plugin.raptor.legacy.RaptorQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRaptorReadOnlySecurity
{
    @Test
    public void testCannotWrite()
            throws Exception
    {
        try (QueryRunner queryRunner = RaptorQueryRunner.builder().addConnectorProperty("raptor.security", "read-only").build()) {
            assertThatThrownBy(() -> {
                queryRunner.execute("CREATE TABLE test_create (a bigint, b double, c varchar)");
            })
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching(".*Access Denied: Cannot create .*");
        }
    }
}
