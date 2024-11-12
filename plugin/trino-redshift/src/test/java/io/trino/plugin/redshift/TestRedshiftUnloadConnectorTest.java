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

import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_REGION;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.S3_UNLOAD_ROOT;
import static io.trino.plugin.redshift.RedshiftQueryRunner.UNLOAD_AWS_ACCESS_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.UNLOAD_AWS_SECRET_KEY;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestRedshiftUnloadConnectorTest
        extends TestRedshiftConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redshift.unload-location", S3_UNLOAD_ROOT)
                .put("redshift.unload-options", "REGION AS '%s' MAXFILESIZE AS 5 MB".formatted(AWS_REGION))
                .put("redshift.iam-role", IAM_ROLE)
                .put("s3.region", AWS_REGION)
                .put("s3.endpoint", "https://s3.%s.amazonaws.com".formatted(AWS_REGION))
                .put("s3.aws-access-key", UNLOAD_AWS_ACCESS_KEY)
                .put("s3.aws-secret-key", UNLOAD_AWS_SECRET_KEY)
                .put("s3.path-style-access", "true")
                .put("join-pushdown.enabled", "true")
                .put("join-pushdown.strategy", "EAGER")
                .buildOrThrow();

        return RedshiftQueryRunner.builder()
                // NOTE this can cause tests to time-out if larger tables like
                //  lineitem and orders need to be re-created.
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setConnectorProperties(properties)
                .build();
    }

    @Test
    @Override
    public void testCancellation()
    {
        abort("java.util.concurrent.TimeoutException: testCancellation() timed out after 60 seconds");
    }
}
