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
package io.prestosql.tests.hive;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.assertions.QueryAssert;
import io.prestosql.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_PARTITIONING;
import static io.prestosql.tests.TestGroups.PRESTO_JDBC;
import static io.prestosql.tests.TestGroups.SMOKE;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSyncPartitionMetadata
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    private HdfsDataSourceWriter hdfsDataSourceWriter;

    @Test(groups = {HIVE_PARTITIONING, SMOKE, PRESTO_JDBC})
    public void testAddPartition()
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");
        assertPartitions(tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertThat(() -> query("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC"))
                .failsWithMessage("Partition location does not exist: hdfs://hadoop-master:9000/user/hive/warehouse/" + tableName + "/col_x=b/col_y=2");
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testDropPartition()
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(tableName, row("a", "1"));
        assertData(tableName, row(1, "a", "1"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testFullSyncPartition()
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("a", "1"), row("f", "9"));
        assertData(tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, PRESTO_JDBC})
    public void testInvalidSyncMode()
    {
        String tableName = "test_repair_invalid_mode";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        assertThat(() -> query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .failsWithMessageMatching("java.sql.SQLException: Query failed (.*): Invalid partition metadata sync mode: INVALID");

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);
        String tableLocation = getTableLocation(tableName, 2);
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/prestosql/tests/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/col_x=h/col_Y=11", dataSource);
        hdfsClient.createDirectory(tableLocation + "/COL_X=UPPER/COL_Y=12");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/COL_X=UPPER/COL_Y=12", dataSource);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL', false)");
        assertPartitions(tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testConflictingMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);
        String tableLocation = getTableLocation(tableName, 2);
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/prestosql/tests/hive/data/single_int_column/data.orc");
        // this conflicts with a partition that already exits in the metastore
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/COL_X=a/cOl_y=1", dataSource);

        assertThatThrownBy(() -> query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table 'default.%s'", tableName));
        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private static void prepare(HdfsClient hdfsClient, HdfsDataSourceWriter hdfsDataSourceWriter, String tableName)
    {
        query("DROP TABLE IF EXISTS " + tableName);

        query("CREATE TABLE " + tableName + " (payload bigint, col_x varchar, col_y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])");
        query("INSERT INTO " + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        String tableLocation = getTableLocation(tableName, 2);
        // remove partition col_x=b/col_y=2
        hdfsClient.delete(tableLocation + "/col_x=b/col_y=2");
        // add partition directory col_x=f/col_y=9 with single_int_column/data.orc file
        hdfsClient.createDirectory(tableLocation + "/col_x=f/col_y=9");
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/prestosql/tests/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/col_x=f/col_y=9", dataSource);
        // should only be picked up when not in case sensitive mode
        hdfsClient.createDirectory(tableLocation + "/COL_X=g/col_y=10");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/COL_X=g/col_y=10", dataSource);

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/col_x=d");
        hdfsClient.createDirectory(tableLocation + "/col_y=3/col_x=h");
        hdfsClient.createDirectory(tableLocation + "/col_y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private static String getTableLocation(String tableName, int partitionLevels)
    {
        String regex = "/[^/]*$";
        for (int i = 0; i < partitionLevels; i++) {
            regex = "/[^/]*" + regex;
        }
        String location = getOnlyElement(onPresto().executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex, tableName)).column(1));
        if (location.startsWith("hdfs://")) {
            try {
                URI uri = new URI(location);
                location = uri.getPath();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        return location;
    }

    private static void cleanup(String tableName)
    {
        query("DROP TABLE " + tableName);
    }

    private static void assertPartitions(String tableName, QueryAssert.Row... rows)
    {
        QueryResult partitionListResult = query("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1, 2");
        assertThat(partitionListResult).containsExactly(rows);
    }

    private static void assertData(String tableName, QueryAssert.Row... rows)
    {
        QueryResult dataResult = query("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactly(rows);
    }
}
