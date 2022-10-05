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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_SECRET_KEY;
import static io.trino.plugin.iceberg.CatalogType.HIVE_METASTORE;
import static io.trino.plugin.iceberg.IcebergConfig.EXTENDED_STATISTICS_CONFIG;
import static java.lang.String.format;

public abstract class BaseIcebergCostBasedPlanTest
        extends BaseCostBasedPlanTest
{
    private static final Logger log = Logger.get(BaseIcebergCostBasedPlanTest.class);

    // Iceberg metadata files are linked using absolute paths, so the bucket name must match where the metadata was exported from.
    // See more at https://github.com/apache/iceberg/issues/1617
    private static final String BUCKET_NAME = "starburst-benchmarks-data";

    // The container needs to be shared, since bucket name cannot be reused between tests.
    // The bucket name is used as a key in TrinoFileSystemCache which is managed in static manner.
    @GuardedBy("sharedDataLakeLock")
    private static HiveMinioDataLake sharedDataLake;
    private static final Object sharedDataLakeLock = new Object();

    protected HiveMinioDataLake dataLake;
    private Map<String, String> connectorConfiguration;

    @Override
    protected ConnectorFactory createConnectorFactory()
    {
        synchronized (sharedDataLakeLock) {
            if (sharedDataLake == null) {
                sharedDataLake = new HiveMinioDataLake(BUCKET_NAME);
                sharedDataLake.start();
            }
            dataLake = sharedDataLake;
        }

        connectorConfiguration = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", HIVE_METASTORE.name())
                .put("hive.metastore.uri", "thrift://" + dataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.endpoint", "http://" + dataLake.getMinio().getMinioApiEndpoint())
                .put("hive.s3.path-style-access", "true")
                .put(EXTENDED_STATISTICS_CONFIG, "true")
                .buildOrThrow();

        return new IcebergConnectorFactory()
        {
            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                checkArgument(config.isEmpty(), "Unexpected configuration %s", config);
                return super.create(catalogName, connectorConfiguration, context);
            }
        };
    }

    @Override
    @BeforeClass
    public void prepareTables()
            throws Exception
    {
        Session session = getQueryRunner().getDefaultSession();

        // Create DistributedQueryRunner to run the setup, as LocalQueryRunner does not support certain statements
        try (DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(connectorConfiguration)
                .build()) {
            session = Session.builder(session)
                    // The persistent LocalQueryRunner may be using a different catalog name than the temporary DistributedQueryRunner one.
                    .setCatalog(queryRunner.getDefaultSession().getCatalog().orElseThrow())
                    .build();
            queryRunner.execute(session, "CREATE SCHEMA " + session.getSchema().orElseThrow());
            prepareTables(queryRunner, session);
        }
    }

    protected abstract void prepareTables(QueryRunner queryRunner, Session session);

    // Iceberg metadata files are linked using absolute paths, so the path within the bucket name must match where the metadata was exported from.
    protected void populateTableFromResource(QueryRunner queryRunner, Session session, String tableName, String resourcePath, String targetPath)
    {
        log.info("Copying resources for %s unpartitioned table from %s to %s in the container", tableName, resourcePath, targetPath);
        dataLake.copyResources(resourcePath, targetPath);
        queryRunner.execute(session, format(
                "CALL iceberg.system.register_table(schema_name => CURRENT_SCHEMA, table_name => '%s', table_location => '%s')",
                tableName,
                "s3://%s/%s".formatted(BUCKET_NAME, targetPath)));
    }

    @AfterClass
    public void cleanUp()
    {
        if (dataLake != null) {
            // Don't stop container, as it's shared
            synchronized (sharedDataLakeLock) {
                verify(dataLake == sharedDataLake);
            }
            dataLake = null;
        }
    }

    @AfterSuite(alwaysRun = true)
    public void disposeSharedResources()
            throws Exception
    {
        synchronized (sharedDataLakeLock) {
            if (sharedDataLake != null) {
                sharedDataLake.stop();
                sharedDataLake = null;
            }
        }
    }
}
