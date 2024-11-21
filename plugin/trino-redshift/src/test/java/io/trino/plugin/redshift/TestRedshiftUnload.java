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
package io.trino.plugin.redshift;

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.tpch.TpchTable.CUSTOMER;

public class TestRedshiftUnload
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestRedshiftUnload.class);

    private static final String IAM_ROLE = requiredNonEmptySystemProperty("test.redshift.iam.role");
    private static final String REGION = requiredNonEmptySystemProperty("test.redshift.aws.region");
    private static final String AWS_ACCESS_KEY = requiredNonEmptySystemProperty("test.redshift.aws.access-key");
    private static final String AWS_SECRET_KEY = requiredNonEmptySystemProperty("test.redshift.aws.secret-key");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .setInitialTables(List.of(CUSTOMER))
                .setConnectorProperties(Map.of(
                        "redshift.unload-location", "s3://starburstdata-engineering-redshift-test/unload",
                        "redshift.iam-role", IAM_ROLE,
                        "s3.region", REGION,
                        "s3.endpoint", "https://s3.%s.amazonaws.com".formatted(REGION),
                        "s3.aws-access-key", AWS_ACCESS_KEY,
                        "s3.aws-secret-key", AWS_SECRET_KEY,
                        "s3.path-style-access", "true"))
                .build();
    }

    @Test
    public void testSelect()
    {
        @Language("SQL")
        String query = """
                       SELECT custkey, name
                       FROM " + TEST_SCHEMA + ".customer
                       WHERE custkey IN (
                         SELECT custkey
                         FROM " + TEST_SCHEMA + ".customer
                         ORDER BY name
                         LIMIT 2
                       )
                       ORDER BY name""";
        assertQuery(query,
                "VALUES (1, 'Customer#000000001'), (2, 'Customer#000000002')");
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", new TestRedshiftUnload().createQueryRunner().getCoordinator().getBaseUrl());
    }
}
