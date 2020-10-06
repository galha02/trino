/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.sqlserver.TestingSqlServer;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestSqlServerTableStatistics
        extends AbstractTestQueryFramework
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        try {
            return createStarburstSqlServerQueryRunner(
                    sqlServer,
                    false,
                    ImmutableMap.of("case-insensitive-name-matching", "true"),
                    ImmutableList.of(ORDERS));
        }
        catch (Throwable e) {
            closeAllSuppress(e, sqlServer::close);
            throw e;
        }
    }

    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderstatus', null, null, null, null, null, null)," +
                            "('totalprice', null, null, null, null, null, null)," +
                            "('orderdate', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('clerk', null, null, null, null, null, null)," +
                            "('shippriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testBasic()
    {
        String tableName = "test_stats_orders";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 120000, 15000, 0, null, null, null)," +
                            "('custkey', 120000, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', 120000, 14996, 0, null, null, null)," +
                            "('orderdate', 45000, 2401, 0, null, null, null)," +
                            "('orderpriority', 252376, 5, 0, null, null, null)," +
                            "('clerk', 450000, 1000, 0, null, null, null)," +
                            "('shippriority', 60000, 1, 0, null, null, null)," +
                            "('comment', 1454727, 14994, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testEmptyTable()
    {
        String tableName = "test_stats_table_empty";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 0, 0, 1, null, null, null)," +
                            "('custkey', 0, 0, 1, null, null, null)," +
                            "('orderpriority', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    orderkey, " +
                        "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                        "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                        "FROM tpch.tiny.orders",
                15000);
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 120000, 15000, 0, null, null, null)," +
                            "('custkey', 80000, 1000, 0.3333333333333333, null, null, null)," +
                            "('orderpriority', 201914, 5, 0.2, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testAverageColumnLength()
    {
        String tableName = "test_stats_table_avg_col_len";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual("" +
                "CREATE TABLE " + tableName + " AS SELECT " +
                "  orderkey, " +
                "  'abc' v3_in_3, " +
                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                "  if(orderkey = 1, '0123456789', NULL) single_10v_value, " +
                "  if(orderkey % 2 = 0, '0123456789', NULL) half_10v_value, " +
                "  if(orderkey % 2 = 0, CAST((1000000 - orderkey) * (1000000 - orderkey) AS varchar(20)), NULL) half_distinct_20v_value, " + // 12 chars each
                "  CAST(NULL AS varchar(10)) all_nulls " +
                "FROM tpch.tiny.orders " +
                "ORDER BY orderkey LIMIT 100");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 800, 100, 0, null, null, null)," +
                            "('v3_in_3', 600, 1, 0, null, null, null)," +
                            "('v3_in_42', 600, 1, 0, null, null, null)," +
                            "('single_10v_value', 20, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 1000, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 1200, 50, 0.5, null, null, null)," +
                            "('all_nulls', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 100, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testPartitionedTable()
    {
        throw new SkipException("Not implemented"); // TODO
    }

    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        sqlServer.execute("CREATE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in SQL Server
        }
        finally {
            sqlServer.execute("DROP VIEW " + tableName);
        }
    }

    @Test
    public void testIndexedView() // materialized view
    {
        String tableName = "test_stats_indexed_view";
        // indexed views require fixed values for several SET options
        try (Handle handle = Jdbi.open(() -> sqlServer.createConnection(""))) {
            // indexed views require fixed values for several SET options
            handle.execute("SET NUMERIC_ROUNDABORT OFF");
            handle.execute("SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON");

            handle.execute("" +
                    "CREATE VIEW " + tableName + " " +
                    "WITH SCHEMABINDING " +
                    "AS SELECT orderkey, custkey, orderpriority, comment FROM dbo.orders");
            try {
                handle.execute("CREATE UNIQUE CLUSTERED INDEX idx1 ON " + tableName + " (orderkey, custkey, orderpriority, comment)");
                gatherStats(tableName);
                assertQuery(
                        "SHOW STATS FOR " + tableName,
                        "VALUES " +
                                "('orderkey', 120000, 15000, 0, null, null, null)," +
                                "('custkey', 120000, 1000, 0, null, null, null)," +
                                "('orderpriority', 252376, 5, 0, null, null, null)," +
                                "('comment', 1454727, 14994, 0, null, null, null)," +
                                "(null, null, null, null, 15000, null, null)");
            }
            finally {
                handle.execute("DROP VIEW " + tableName);
            }
        }
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        sqlServer.execute("" +
                "SELECT " +
                "  orderkey CASE_UNQUOTED_UPPER, " +
                "  custkey case_unquoted_lower, " +
                "  orderstatus cASe_uNQuoTeD_miXED, " +
                "  totalprice \"CASE_QUOTED_UPPER\", " +
                "  orderdate \"case_quoted_lower\", " +
                "  orderpriority \"CasE_QuoTeD_miXED\" " +
                "INTO " + tableName + " " +
                "FROM orders");
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', 120000, 15000, 0, null, null, null)," +
                            "('case_unquoted_lower', 120000, 1000, 0, null, null, null)," +
                            "('case_unquoted_mixed', 30000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', 120000, 14996, 0, null, null, null)," +
                            "('case_quoted_lower', 45000, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', 252376, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            sqlServer.execute("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] testCaseColumnNamesDataProvider()
    {
        return new Object[][] {
                {"TEST_STATS_MIXED_UNQUOTED_UPPER"},
                {"test_stats_mixed_unquoted_lower"},
                {"test_stats_mixed_uNQuoTeD_miXED"},
                {"\"TEST_STATS_MIXED_QUOTED_UPPER\""},
                {"\"test_stats_mixed_quoted_lower\""},
                {"\"test_stats_mixed_QuoTeD_miXED\""},
        };
    }

    private void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        for (Object columnName : columnNames) {
            sqlServer.execute(format("CREATE STATISTICS %1$s ON %2$s (%1$s)", columnName, tableName));
        }
        sqlServer.execute("UPDATE STATISTICS " + tableName);
    }
}
