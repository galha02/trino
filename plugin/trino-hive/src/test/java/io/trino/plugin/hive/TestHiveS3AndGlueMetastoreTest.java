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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveS3AndGlueMetastoreTest
        extends BaseS3AndGlueMetastoreTest
{
    public TestHiveS3AndGlueMetastoreTest()
    {
        super("partitioned_by", "external_location", requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        metastore = createTestingGlueHiveMetastore(Path.of(schemaPath()));

        Session session = createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin"))));
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(session)
                .setCreateTpchSchemas(false)
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.non-managed-table-writes-enabled", "true")
                .setMetastore(runner -> metastore)
                .build();
        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaPath() + "')");
        return queryRunner;
    }

    private Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of()))
                        .build())
                .setCatalog("hive")
                .setSchema(schemaName)
                .build();
    }

    @Override
    protected Session sessionForOptimize()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "non_transactional_optimize_enabled", "true")
                .build();
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + locationDirectory + partitionPart + "[a-zA-Z0-9_-]+$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        // No metadata files for Hive
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return new HashSet<>(getTableFiles(tableLocation));
    }

    @Override
    protected void validateFilesAfterOptimize(String location, Set<String> initialFiles, Set<String> updatedFiles)
    {
        assertThat(updatedFiles).hasSizeLessThan(initialFiles.size());
        assertThat(getAllDataFilesFromTableDirectory(location)).isEqualTo(updatedFiles);
    }

    @Override // Row-level modifications are not supported for Hive tables
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_basic_operations_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

        String actualTableLocation = getTableLocation(tableName);
        String expectedLocation = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
        assertThat(actualTableLocation).isEqualTo(expectedLocation);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
        validateDataFiles(partitioned ? "col_int" : "", tableName, actualTableLocation);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override // Row-level modifications are not supported for Hive tables
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedSchemaLocation(boolean partitioned, String locationPattern)
    {
        String schemaName = "test_basic_operations_schema_" + randomNameSuffix();
        String schemaLocation = locationPattern.formatted(bucketName, schemaName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;
        String partitionQueryPart = (partitioned ? " WITH (partitioned_by = ARRAY['col_int'])" : "");

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

        assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str varchar, col_int int)" + partitionQueryPart);
        String expectedTableLocation = (schemaLocation.endsWith("/") ? schemaLocation : schemaLocation + "/") + tableName;

        String actualTableLocation = metastore.getTable(schemaName, tableName).orElseThrow().getStorage().getLocation();
        assertThat(actualTableLocation).matches(expectedTableLocation);

        assertUpdate("INSERT INTO " + qualifiedTableName + "  VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        assertQuery("SELECT * FROM " + qualifiedTableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

        assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
        validateDataFiles(partitioned ? "col_int" : "", qualifiedTableName, actualTableLocation);

        assertUpdate("DROP TABLE " + qualifiedTableName);
        assertThat(getTableFiles(actualTableLocation)).isEmpty();

        assertUpdate("DROP SCHEMA " + schemaName);
        validateFilesAfterDrop(actualTableLocation);
    }

    @Override
    @Test(dataProvider = "locationPatternsDataProvider")
    public void testMergeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        // Row-level modifications are not supported for Hive tables
    }

    @Test(dataProvider = "locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, schemaName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        //Check statistics collection on write
        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 0.0, 1.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 16.0, 3.0, 0.0, null, null, null),
                    ('col_int', null, 3.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }

        //Check statistics collection explicitly
        assertUpdate("ANALYZE " + tableName, 4);

        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 16.0, 1.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 16.0, 4.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + "(col_str varchar, col_int integer) WITH (external_location = '" + location + "')"))
                .hasMessageContaining("External location is not a valid file system URI")
                .hasStackTraceContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCtasWithIncorrectLocation()
    {
        String tableName = "test_ctas_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/%s/a#hash/%s".formatted(bucketName, schemaName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                " WITH (external_location = '" + location + "')" +
                " AS VALUES ('str1', 1)"))
                .hasMessageContaining("External location is not a valid file system URI")
                .hasStackTraceContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCreateSchemaWithIncorrectLocation()
    {
        String schemaName = "test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String schemaLocation = "s3://%s/%2$s/a#hash/%2$s".formatted(bucketName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str, col_int) AS VALUES ('str1', 1)"))
                .hasMessageContaining("Fragment is not allowed in a file system location");

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str varchar, col_int integer)"))
                .hasMessageContaining("Fragment is not allowed in a file system location");

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testSchemaNameEscape()
    {
        String schemaNameSuffix = randomNameSuffix();
        String schemaName = "../test_create_schema_escaped_" + schemaNameSuffix;
        String tableName = "test_table_schema_escaped_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA \"%2$s\" WITH (location = 's3://%1$s/%2$s')".formatted(bucketName, schemaName));
        assertQueryFails("CREATE TABLE \"" + schemaName + "\"." + tableName + " (col) AS VALUES 1", "Failed checking path: .*");

        assertUpdate("DROP SCHEMA \"" + schemaName + "\"");
    }

    @Override
    protected List<String> locationPatterns()
    {
        // TODO https://github.com/trinodb/trino/issues/17803 Fix correctness issue with double slash
        return super.locationPatterns().stream()
                .filter(location -> !location.contains("double_slash"))
                .collect(toImmutableList());
    }
}
