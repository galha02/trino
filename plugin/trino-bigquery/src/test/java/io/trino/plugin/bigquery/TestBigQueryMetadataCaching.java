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
package io.trino.plugin.bigquery;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryMetadataCaching
        extends AbstractTestQueryFramework
{
    protected BigQuerySqlExecutor bigQuerySqlExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(Map.of("bigquery.metadata.cache-ttl", "5m"))
                .build();
    }

    @Test
    public void testMetadataCaching()
    {
        String schema = "test_metadata_caching_" + randomNameSuffix();
        try {
            getQueryRunner().execute("CREATE SCHEMA " + schema);
            assertThat(getQueryRunner().execute("SHOW SCHEMAS IN bigquery LIKE '" + schema + "'").getOnlyValue()).isEqualTo(schema);

            String schemaTableName = schema + ".test_metadata_caching";
            getQueryRunner().execute("CREATE TABLE " + schemaTableName + " AS SELECT * FROM tpch.tiny.region");
            assertThat(getQueryRunner().execute("SELECT * FROM " + schemaTableName).getRowCount()).isEqualTo(5);

            bigQuerySqlExecutor.execute("DROP SCHEMA " + schema + " CASCADE");
            assertThat(getQueryRunner().execute("SHOW SCHEMAS IN bigquery LIKE '" + schema + "'").getOnlyValue()).isEqualTo(schema);

            assertQueryFails("SELECT * FROM " + schemaTableName, ".*Schema '.+' does not exist.*");
        }
        finally {
            bigQuerySqlExecutor.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }
}
