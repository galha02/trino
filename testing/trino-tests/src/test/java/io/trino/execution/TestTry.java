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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.memory.MemoryQueryRunner.createMemoryQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTry
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMemoryQueryRunner(ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testTryWithColumnWithAt()
    {
        assertUpdate("CREATE TABLE table_with_column_with_at(\"a@b\" integer)");
        assertUpdate("INSERT INTO table_with_column_with_at VALUES (55)", 1);
        assertThat(query("SELECT try(\"a@b\") FROM table_with_column_with_at"))
                .matches("VALUES 55");
        assertUpdate("DROP TABLE table_with_column_with_at");
    }
}
