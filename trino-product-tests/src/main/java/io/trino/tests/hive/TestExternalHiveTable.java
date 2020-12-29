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
package io.trino.tests.hive;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.fulfillment.table.TableInstance;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.prestosql.tempto.Requirements.compose;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.trino.tests.TestGroups.HIVE_WITH_EXTERNAL_WRITES;
import static io.trino.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static io.trino.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT;
import static io.trino.tests.utils.QueryExecutors.onHive;
import static io.trino.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;

public class TestExternalHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String HIVE_CATALOG_WITH_EXTERNAL_WRITES = "hive_with_external_writes";
    private static final String EXTERNAL_TABLE_NAME = "target_table";

    @Inject
    private HdfsClient hdfsClient;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                mutableTable(NATION),
                mutableTable(NATION_PARTITIONED_BY_BIGINT_REGIONKEY));
    }

    @Test
    public void testShowStatisticsForExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);

        onHive().executeQuery("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS");
        assertThat(query("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));

        onHive().executeQuery("ANALYZE TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
        assertThat(query("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    public void testAnalyzeExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);

        // Running ANALYZE on an external table is allowed as long as the user has the privileges.
        assertThat(query("ANALYZE hive.default." + EXTERNAL_TABLE_NAME)).containsExactly(row(5));
    }

    @Test
    public void testInsertIntoExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery(
                "INSERT INTO hive.default." + EXTERNAL_TABLE_NAME + " SELECT * FROM hive.default." + nation.getNameInDatabase()))
                .failsWithMessage("Cannot write to non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase());
        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME))
                .failsWithMessage("Cannot delete from non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalPartitionedTableTable()
    {
        TableInstance<?> nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery("CREATE EXTERNAL TABLE " + EXTERNAL_TABLE_NAME + " LIKE " + nation.getNameInDatabase() + " LOCATION '/tmp/" + EXTERNAL_TABLE_NAME + "_" + nation.getNameInDatabase() + "'");
        insertNationPartition(nation, 1);
        insertNationPartition(nation, 2);
        insertNationPartition(nation, 3);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(3 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_name IS NOT NULL"))
                .failsWithMessage("Deletes must match whole partitions for non-transactional tables");

        onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME + " WHERE p_regionkey = 1");
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(2 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME)).hasRowsCount(0);
    }

    @Test(groups = {HIVE_WITH_EXTERNAL_WRITES, PROFILE_SPECIFIC_TESTS})
    public void testCreateExternalTableWithInaccessibleSchemaLocation()
    {
        String schema = "schema_without_location";
        String schemaLocation = "/tmp/" + schema;
        hdfsClient.createDirectory(schemaLocation);
        query(format("CREATE SCHEMA %s.%s WITH (location='%s')", HIVE_CATALOG_WITH_EXTERNAL_WRITES, schema, schemaLocation));

        hdfsClient.delete(schemaLocation);

        String table = "test_create_external";
        String tableLocation = "/tmp/" + table;
        query(format("CREATE TABLE %s.%s.%s WITH (external_location = '%s') AS SELECT * FROM tpch.tiny.nation", HIVE_CATALOG_WITH_EXTERNAL_WRITES, schema, table, tableLocation));
    }

    private void insertNationPartition(TableInstance<?> nation, int partition)
    {
        onHive().executeQuery(
                "INSERT INTO TABLE " + EXTERNAL_TABLE_NAME + " PARTITION (p_regionkey=" + partition + ")"
                        + " SELECT p_nationkey, p_name, p_comment FROM " + nation.getNameInDatabase()
                        + " WHERE p_regionkey=" + partition);
    }
}
