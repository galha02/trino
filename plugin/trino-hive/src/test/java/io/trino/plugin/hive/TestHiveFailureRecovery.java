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

import io.trino.Session;
import io.trino.testing.AbstractTestFailureRecovery;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveFailureRecovery
        extends AbstractTestFailureRecovery
{
    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(requiredTpchTables)
                .setInitialTablesSessionMutator(
                        // create initial tables with retries disbabled (write operations do not support retries yet)
                        session -> Session.builder(session)
                                .setSystemProperty("retry_policy", "NONE")
                                .build())
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(configProperties)
                .build();
    }

    @Override
    protected void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (format = 'TEXTFILE', partitioned_by=array['%s']) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumn,
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return false;
    }

    @Override
    // create table is not atomic at the moment
    @Test(enabled = false)
    public void testCreateTable()
    {
        super.testCreateTable();
    }

    @Override
    // delete is unsupported for non ACID tables
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasMessageContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    // delete is unsupported for non ACID tables
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDelete)
                .hasMessageContaining("Deletes must match whole partitions for non-transactional tables");
    }

    @Override
    // update is unsupported for non ACID tables
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("Hive update is only supported for ACID transactional tables");
    }

    @Override
    // update is unsupported for non ACID tables
    public void testUpdateWithSubquery()
    {
        assertThatThrownBy(super::testUpdateWithSubquery)
                .hasMessageContaining("Hive update is only supported for ACID transactional tables");
    }

    @Override
    // materialized views are currently not implemented by Hive connector
    public void testRefreshMaterializedView()
    {
        assertThatThrownBy(super::testRefreshMaterializedView)
                .hasMessageContaining("This connector does not support creating materialized views");
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // create table is not atomic at the moment
    public void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // create partition is not atomic at the moment
    public void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT, enabled = false)
    // replace partition is not atomic at the moment
    public void testReplaceExistingPartition()
    {
        testTableModification(
                Optional.of(Session.builder(getQueryRunner().getDefaultSession())
                        .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                        .build()),
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testDeletePartitionWithSubquery()
    {
        assertThatThrownBy(() -> {
            testTableModification(
                    Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 0 p FROM orders"),
                    "DELETE FROM <table> WHERE p = (SELECT min(nationkey) FROM nation)",
                    Optional.of("DROP TABLE <table>"));
        }).hasMessageContaining("Deletes must match whole partitions for non-transactional tables");
    }
}
