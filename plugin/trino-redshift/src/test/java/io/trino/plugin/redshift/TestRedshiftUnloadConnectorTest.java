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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_REGION;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.S3_UNLOAD_ROOT;
import static io.trino.plugin.redshift.RedshiftQueryRunner.UNLOAD_AWS_ACCESS_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.UNLOAD_AWS_SECRET_KEY;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_DATABASE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestRedshiftUnloadConnectorTest
        extends TestRedshiftConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        redshiftJdbcConfig = new RedshiftConfig().setUnloadLocation(S3_UNLOAD_ROOT).setIamRole(IAM_ROLE);
        redshiftUnloadSplitManager = createSplitManager();

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

    @Test
    @Override
    void testJdbcFlow()
    {
        abort("Not applicable for unload test");
    }

    @Test
    void testUnloadFlow()
            throws ExecutionException, InterruptedException
    {
        SchemaTableName schemaTableName = new SchemaTableName(TEST_SCHEMA, "nation");
        JdbcTableHandle table = new JdbcTableHandle(schemaTableName, new RemoteTableName(Optional.of(TEST_DATABASE), Optional.of(TEST_SCHEMA), "nation"), Optional.empty());

        ConnectorSession session = createUnloadSession();
        ConnectorSplitSource splitSource = redshiftUnloadSplitManager.getSplits(null, session, table, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        assertThat(splitSource).isInstanceOf(RedshiftUnloadSplitSource.class);

        ConnectorSplitSource.ConnectorSplitBatch connectorSplitBatch = splitSource.getNextBatch(2).get();
        assertThat(connectorSplitBatch.isNoMoreSplits()).isTrue();
        assertThat(connectorSplitBatch.getSplits()).hasSize(2);
        connectorSplitBatch.getSplits().forEach(split -> assertThat(split).isInstanceOf(RedshiftUnloadSplit.class));
        verifyS3UnloadedFiles(connectorSplitBatch.getSplits().stream().map(split -> ((RedshiftUnloadSplit) split).path()).toList());
    }

    @Test
    void testUnloadFlowFallbackToJdbc()
            throws ExecutionException, InterruptedException
    {
        SchemaTableName schemaTableName = new SchemaTableName(TEST_SCHEMA, "nation");
        JdbcTableHandle table = new JdbcTableHandle(
                new JdbcNamedRelationHandle(schemaTableName, new RemoteTableName(Optional.of(TEST_DATABASE), Optional.of(TEST_SCHEMA), "nation"), Optional.empty()),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.of(2),
                Optional.empty(),
                Optional.of(ImmutableSet.of()),
                0,
                Optional.empty(),
                ImmutableList.of());

        ConnectorSession session = createUnloadSession();
        try (ConnectorSplitSource splitSource = redshiftUnloadSplitManager.getSplits(null, session, table, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            assertThat(splitSource).isInstanceOf(FixedSplitSource.class);

            ConnectorSplitSource.ConnectorSplitBatch connectorSplitBatch = splitSource.getNextBatch(2).get();
            assertThat(connectorSplitBatch.isNoMoreSplits()).isTrue();
            assertThat(connectorSplitBatch.getSplits()).hasSize(1);
            connectorSplitBatch.getSplits().forEach(split -> assertThat(split).isInstanceOf(JdbcSplit.class));
        }
    }

    private static void verifyS3UnloadedFiles(List<String> files)
    {
        files.forEach(file -> assertThat(file).startsWith(S3_UNLOAD_ROOT));
        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(UNLOAD_AWS_ACCESS_KEY, UNLOAD_AWS_SECRET_KEY)))
                .region(Region.US_EAST_1)
                .build()) {
            files.forEach(
                    file -> {
                        URI s3Path = URI.create(file);
                        String bucket = s3Path.getHost();
                        String key = s3Path.getPath().substring(1);
                        assertThat(s3.headObject(request -> request.bucket(bucket).key(key)).contentLength()).isGreaterThan(0);
                        s3.deleteObject(request -> request.bucket(bucket).key(key));
                    });
        }
    }
}
