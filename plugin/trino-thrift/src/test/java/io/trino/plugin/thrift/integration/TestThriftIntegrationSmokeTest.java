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
package io.trino.plugin.thrift.integration;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.QueryAssertions.assertContains;

public class TestThriftIntegrationSmokeTest
        // TODO extend BaseConnectorTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createThriftQueryRunner(2, false, ImmutableMap.of());
    }

    @Override
    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("tiny")
                .row("sf1");
        assertContains(actualSchemas, resultBuilder.build());
    }
}
