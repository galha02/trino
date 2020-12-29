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

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.tpch.TpchTable.NATION;

public class TestShowStats
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                // create nation so tpch schema got created
                .setInitialTables(ImmutableList.of(NATION))
                .setNodeCount(1)
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE nation_partitioned(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey BIGINT) WITH (partitioned_by = ARRAY['regionkey'])");
        assertUpdate("INSERT INTO nation_partitioned SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation", 25);
        assertUpdate("CREATE TABLE region AS SELECT * FROM tpch.tiny.region", 5);
        assertUpdate("CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders", 15000);
    }

    @Test
    public void testShowStats()
    {
        assertQuery(
                "SHOW STATS FOR nation_partitioned",
                "VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned)",
                "VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NOT NULL)",
                "VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IS NULL)",
                "VALUES " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey = 1)",
                "VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 1, 1), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 38.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 500.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey IN (1, 3))",
                "VALUES " +
                        "   ('regionkey', null, 2.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 78.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 847.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 10.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)",
                "VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 109.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1199.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 15.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 3)",
                "VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 4, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 4, 20), " +
                        "   ('name', 31.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 348.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey < 1)",
                "VALUES " +
                        "   ('regionkey', null, 1.0, 0.0, null, 0, 0), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 16), " +
                        "   ('name', 37.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 310.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 5.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 0 and regionkey < 4)",
                "VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   ('name', 109.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1199.0, 5.0, 0.0, null, null, null), " +
                        "   (null, null, null, null, 15.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)",
                "VALUES " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT *, * FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)",
                "VALUES " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT *, *, regionkey FROM nation_partitioned WHERE regionkey > 10 or regionkey < 0)",
                "VALUES " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('nationkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('name', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('comment', 0.0, 0.0, 1.0, null, null, null), " +
                        "   ('regionkey', 0.0, 0.0, 1.0, null, null, null), " +
                        "   (null, null, null, null, 0.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT *, regionkey FROM nation_partitioned)",
                "VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0.0, null, null, null), " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   (null, null, null, null, 25.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT *, * FROM nation_partitioned)",
                "VALUES " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857, 5.0, 0.0, null, null, null), " +
                        "   ('regionkey', null, 5.0, 0.0, null, 0, 4), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 0, 24), " +
                        "   ('name', 177.0, 5.0, 0.0, null, null, null), " +
                        "   ('comment', 1857.0, 5.0, 0, null, null, null), " +
                        "   (null, null, null, null, 25.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT regionkey FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)",
                "VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   (null, null, null, null, 15.0, null, null)");

        assertQuery(
                "SHOW STATS FOR (SELECT regionkey, nationkey FROM nation_partitioned WHERE regionkey BETWEEN 1 AND 1 + 2)",
                "VALUES " +
                        "   ('regionkey', null, 3.0, 0.0, null, 1, 3), " +
                        "   ('nationkey', null, 5.0, 0.0, null, 1, 24), " +
                        "   (null, null, null, null, 15.0, null, null)");
    }

    @Test
    public void testShowStatsWithoutFrom()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT 1 AS x)",
                "VALUES " +
                        "   ('x', null, 1.0, 0.0, null, 1, 1), " +
                        "   (null, null, null, null, 1.0, null, null)");
    }

    @Test
    public void testShowStatsWithMultipleFrom()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned, region)",
                "VALUES " +
                        "   ('nationkey', null, 5, 0, null, 0, 24), " +
                        "   ('name',      850, 5, 0, null, null, null), " +
                        "   ('comment',   9285, 5, 0, null, null, null), " +
                        // The two regionkey columns come from two different tables
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   ('name',      885, 5, 0, null, null, null), " +
                        "   ('comment',   8250, 5, 0, null, null, null), " +
                        "   (null, null, null, null, 125, null, null)");
    }

    @Test
    public void testShowStatsWithoutTableScanAndImplicitColumnNames()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM (VALUES 1))",
                "VALUES " +
                        "   ('_col0', null, 1, 0, null, 1, 1), " +
                        "   (null, null, null, null, 1, null, null)");
    }

    @Test
    public void testShowStatsWithSubquery()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM (SELECT * FROM nation))",
                "VALUES " +
                        "   ('nationkey', null, 25, 0, null, 0, 24), " +
                        "   ('name', 177, 25, 0, null, null, null), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   ('comment', 1857, 25, 0, null, null, null), " +
                        "   (null, null, null, null, 25, null, null)");
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation, (SELECT * FROM nation))",
                "VALUES " +
                        "   ('nationkey', null, 25, 0, null, 0, 24), " +
                        "   ('name', 4425, 25, 0, null, null, null), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   ('comment', 46425, 25, 0, null, null, null), " +
                        "   ('nationkey', null, 25, 0, null, 0, 24), " +
                        "   ('name', 4425, 25, 0, null, null, null), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   ('comment', 46425, 25, 0, null, null, null), " +
                        "   (null, null, null, null, 625, null, null)");
    }

    @Test
    public void testShowStatsForNonExistingColumnFails()
    {
        assertQueryFails("SHOW STATS FOR (SELECT column_does_not_exist FROM nation_partitioned)", ".*Column 'column_does_not_exist' cannot be resolved");
    }

    @Test
    public void testShowStatsForNonColumnQuery()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT 1 AS x FROM nation_partitioned)",
                "VALUES " +
                        "   ('x', null, 1, 0, null, 1, 1), " +
                        "   (null, null, null, null, 25, null, null)");
    }

    @Test
    public void testShowStatsForAliasedColumnQuery()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT nationkey, name, name AS name2 FROM nation_partitioned WHERE regionkey > 0 and regionkey < 4)",
                "VALUES " +
                        "   ('nationkey', null, 5, 0, null, 1, 24), " +
                        "   ('name', 109, 5, 0, null, null, null), " +
                        "   ('name2', 109, 5, 0, null, null, null), " +
                        "   (null, null, null, null, 15, null, null)");
    }

    @Test
    public void testShowStatsForNonIdentifierColumn()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT CONCAT('some', 'value') AS x FROM nation_partitioned)",
                "VALUES " +
                        "   ('x', null, 1, 0, null, null, null), " +
                        "   (null, null, null, null, 25, null, null)");
    }

    @Test
    public void testShowStatsForColumnExpression()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT nationkey + 100 AS x FROM nation_partitioned)",
                "VALUES " +
                        "   ('x', null, 5, 0, null, 100, 124), " +
                        "   (null, null, null, null, 25, null, null)");
    }

    @Test
    public void testShowStatsWithGroupBy()
    {
        // TODO calculate row count - https://github.com/prestosql/presto/issues/6323
        assertQuery(
                "SHOW STATS FOR (SELECT avg(totalprice) AS x FROM orders GROUP BY orderkey)",
                "VALUES " +
                        "   ('x', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
    }

    @Test
    public void testShowStatsWithHaving()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT count(nationkey) AS x FROM nation_partitioned GROUP BY regionkey HAVING regionkey > 0)",
                "VALUES " +
                        "   ('x', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
    }

    @Test
    public void testShowStatsWithSelectDistinct()
    {
        // TODO calculate row count - https://github.com/prestosql/presto/issues/6323
        assertQuery(
                "SHOW STATS FOR (SELECT DISTINCT * FROM orders)",
                "VALUES " +
                        "   ('orderkey', null, null, null, null, null, null), " +
                        "   ('custkey', null, null, null, null, null, null), " +
                        "   ('orderstatus', null, null, null, null, null, null), " +
                        "   ('totalprice', null, null, null, null, null, null), " +
                        "   ('orderdate', null, null, null, null, null, null), " +
                        "   ('orderpriority', null, null, null, null, null, null), " +
                        "   ('clerk', null, null, null, null, null, null), " +
                        "   ('shippriority', null, null, null, null, null, null), " +
                        "   ('comment', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
        // TODO calculate row count - https://github.com/prestosql/presto/issues/6323
        assertQuery(
                "SHOW STATS FOR (SELECT DISTINCT regionkey FROM region)",
                "VALUES " +
                        "   ('regionkey', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
    }

    @Test
    public void testShowStatsWithSelectFunctionCall()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT sin(orderkey) AS x FROM orders)",
                "VALUES " +
                        "   ('x', null, null, null, null, null, null), " +
                        "   (null, null, null, null, 15000, null, null)");
        // TODO - row count should be 1 - https://github.com/prestosql/presto/issues/6323
        assertQuery(
                "SHOW STATS FOR (SELECT count(*) AS x FROM orders)",
                "VALUES " +
                        "   ('x', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
    }

    @Test
    public void testShowStatsWithPredicate()
    {
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey + 100 < 200)",
                "VALUES " +
                        "   ('nationkey', null, 5, 0, null, 0, 24), " +
                        "   ('name', 177, 5, 0, null, null, null), " +
                        "   ('comment', 1857, 5, 0, null, null, null), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   (null, null, null, null, 25, null, null)");
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE regionkey > 0 and nationkey > 0)",
                "VALUES " +
                        "   ('nationkey', null, 5, 0, null, 1, 24), " +
                        "   ('name', 140, 5, 0, null, null, null), " +
                        "   ('comment', 1547, 5, 0, null, null, null), " +
                        "   ('regionkey', null, 4, 0, null, 1, 4), " +
                        "   (null, null, null, null, 20, null, null)");
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE nationkey = 1 and name is not null)",
                "VALUES " +
                        "   ('nationkey', null, 1, 0, null, 1, 1), " +
                        "   ('name', 35.4, 5, 0, null, null, null), " +
                        "   ('comment', 371.4, 5, 0, null, null, null), " +
                        "   ('regionkey', null, 5, 0, null, 0, 4), " +
                        "   (null, null, null, null, 5, null, null)");
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_partitioned WHERE sin(regionkey) > 0)",
                "VALUES " +
                        "   ('nationkey', null, null, null, null, null, null), " +
                        "   ('name', null, null, null, null, null, null), " +
                        "   ('comment', null, null, null, null, null, null), " +
                        "   ('regionkey', null, null, null, null, null, null), " +
                        "   (null, null, null, null, null, null, null)");
    }

    @Test
    public void testShowStatsWithView()
    {
        assertUpdate("CREATE VIEW nation_view AS SELECT * FROM tpch.tiny.nation WHERE nationkey % 5 = 0");
        assertQuery(
                "SHOW STATS FOR nation_view",
                "VALUES " +
                        "   ('nationkey', null, 1, 0, null, 0, 24), " +
                        "   ('name', 7.08, 1, 0, null, null, null), " +
                        "   ('comment', 74.28, 1, 0, null, null, null), " +
                        "   ('regionkey', null, 1, 0, null, 0, 4), " +
                        "   (null, null, null, null, 1, null, null)");
        assertQuery(
                "SHOW STATS FOR (SELECT * FROM nation_view WHERE regionkey = 0)",
                "VALUES " +
                        "   ('nationkey', null, 1, 0, null, 0, 24), " +
                        "   ('name', 7.08, 1, 0, null, null, null), " +
                        "   ('comment', 74.28, 1, 0, null, null, null), " +
                        "   ('regionkey', null, 1, 0, null, 0, 0), " +
                        "   (null, null, null, null, 1, null, null)");
        assertUpdate("DROP VIEW nation_view");
    }
}
