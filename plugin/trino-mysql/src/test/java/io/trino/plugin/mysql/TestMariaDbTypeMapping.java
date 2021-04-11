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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;

import static io.trino.plugin.mysql.MariaDbQueryRunner.createMariaDbQueryRunner;

public class TestMariaDbTypeMapping
        extends BaseMySqlTypeMappingTest
{
    private TestingMariaDbServer mariaDbServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mariaDbServer = closeAfterClass(new TestingMariaDbServer());
        return createMariaDbQueryRunner(mariaDbServer, ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of());
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return mariaDbServer::execute;
    }
}
