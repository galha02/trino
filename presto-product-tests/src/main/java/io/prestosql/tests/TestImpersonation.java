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
package io.prestosql.tests;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tests.TestGroups.HDFS_IMPERSONATION;
import static io.prestosql.tests.TestGroups.HDFS_NO_IMPERSONATION;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.utils.QueryExecutors.connectToPresto;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestImpersonation
        extends ProductTest
{
    private QueryExecutor aliceExecutor;

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.alice@presto.jdbc_user")
    private String aliceJdbcUser;

    // The value for configuredHdfsUser is profile dependent
    // For non-Kerberos environments this variable will be equal to -DHADOOP_USER_NAME as set in jvm.config
    // For Kerberized environments this variable will be equal to the hive.hdfs.presto.principal property as set in hive.properties
    @Inject
    @Named("databases.presto.configured_hdfs_user")
    private String configuredHdfsUser;

    @BeforeTestWithContext
    public void setup()
    {
        aliceExecutor = connectToPresto("alice@presto");
    }

    @Test(groups = {HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableHdfsImpersonationDisabled()
    {
        String tableName = "create_hdfs_impersonation_disabled";
        checkTableOwner(tableName, configuredHdfsUser, aliceExecutor);
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableHdfsImpersonationEnabled()
    {
        String tableName = "create_hdfs_impersonation_enabled";
        checkTableOwner(tableName, aliceJdbcUser, aliceExecutor);
    }

    private static String getTableLocation(QueryExecutor executor, String tableName)
    {
        String location = getOnlyElement(executor.executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName)).column(1));
        if (location.startsWith("hdfs://")) {
            try {
                URI uri = new URI(location);
                return uri.getPath();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return location;
    }

    private void checkTableOwner(String tableName, String expectedOwner, QueryExecutor executor)
    {
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        executor.executeQuery(format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
        String tableLocation = getTableLocation(executor, tableName);
        String owner = hdfsClient.getOwner(tableLocation);
        assertEquals(owner, expectedOwner);
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }
}
