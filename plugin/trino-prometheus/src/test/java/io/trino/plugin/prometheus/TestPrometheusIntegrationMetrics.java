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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.prometheus.PrometheusQueryRunner.createPrometheusQueryRunner;

public class TestPrometheusIntegrationMetrics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        PrometheusServer server = closeAfterClass(new PrometheusServer());
        return createPrometheusQueryRunner(server, ImmutableMap.of());
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES IN default LIKE 'up'", "VALUES 'up'");
    }

    @Test
    public void testShowCreateSchema()
    {
        assertQuery("SHOW CREATE SCHEMA default", "VALUES 'CREATE SCHEMA prometheus.default'");
        assertQueryFails("SHOW CREATE SCHEMA unknown", ".*Schema 'prometheus.unknown' does not exist");
    }

    @Test
    public void testListSchemaNames()
    {
        assertQuery("SHOW SCHEMAS LIKE 'default'", "VALUES 'default'");
    }

    @Test
    public void testCreateTable()
    {
        assertQueryFails("CREATE TABLE default.foo (text VARCHAR)", "This connector does not support creating tables");
    }

    @Test
    public void testDropTable()
    {
        assertQueryFails("DROP TABLE default.up", "This connector does not support dropping tables");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuery("DESCRIBE default.up",
                "VALUES " +
                        "('labels', 'map(varchar, varchar)', '', '')," +
                        "('timestamp', 'timestamp(3) with time zone', '', '')," +
                        "('value', 'double', '', '')");
    }

    @Test
    public void testSelectWithoutLabels()
    {
        assertQuerySucceeds("SELECT timestamp, value FROM prometheus.default.up");
        assertQuery("SELECT value FROM prometheus.default.up LIMIT 1", "VALUES ('1.0')");
    }
}
