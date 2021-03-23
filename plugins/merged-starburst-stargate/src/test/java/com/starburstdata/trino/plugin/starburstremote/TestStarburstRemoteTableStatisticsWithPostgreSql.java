/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithPostgreSql;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;

public class TestStarburstRemoteTableStatisticsWithPostgreSql
        extends AbstractTestQueryFramework
{
    private TestingPostgreSqlServer postgreSqlServer;
    private DistributedQueryRunner remoteStarburst;
    private Session remoteSession;
    private H2QueryRunner h2QueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        h2QueryRunner = closeAfterClass(new H2QueryRunner());
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(),
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword(),
                        "case-insensitive-name-matching", "true"),
                ImmutableList.of(ORDERS, NATION),
                Optional.empty()));
        remoteSession = testSessionBuilder()
                .setCatalog("postgresql")
                .setSchema("tiny")
                .build();
        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "postgresql"),
                        "allow-drop-table", "true",
                        "case-insensitive-name-matching", "true"));
    }

    @Test(invocationCount = 10, successPercentage = 50) // PostgreSQL can auto-analyze data before we SHOW STATS
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            assertLocalAndRemoteStatistics(
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
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, null, null)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', 135000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, null, null)," +
                            "('comment', 735000, 14995, 0, null, null, null)," +
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
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 0, 0, 1, null, null, null)," +
                            "('custkey', 0, 0, 1, null, null, null)," +
                            "('orderpriority', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 0, null, null)");
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
        executeInRemoteStarburst(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            executeInRemoteStarburst(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
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
        executeInRemoteStarburst("" +
                "CREATE TABLE " + tableName + " AS " +
                "SELECT " +
                "    orderkey, " +
                "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                "FROM tpch.tiny.orders");
        assertQuery("SELECT COUNT(*) FROM " + tableName, "VALUES 15000");
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0.3333333333333333, null, null, null)," +
                            "('orderpriority', 108000, 5, 0.2, null, null, null)," +
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
        executeInRemoteStarburst("" +
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
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 100, 0, null, null, null)," +
                            "('v3_in_3', 400, 1, 0, null, null, null)," +
                            "('v3_in_42', 400, 1, 0, null, null, null)," +
                            "('single_10v_value', 11, 1, 0.99, null, null, null)," +
                            "('half_10v_value', 550, 1, 0.5, null, null, null)," +
                            "('half_distinct_20v_value', 650, 50, 0.5, null, null, null)," +
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
        throw new SkipException("Not implemented"); // TODO upgrade testing PostgreSQL server to newer version
    }

    @Test
    public void testPartitionedTableWithPredicate()
    {
        throw new SkipException("https://starburstdata.atlassian.net/browse/PRESTO-5011"); // TODO https://starburstdata.atlassian.net/browse/PRESTO-5011
    }

    @Test
    public void testView()
    {
        String tableName = "test_stats_view";
        executeInPostgres("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in PostgreSQL
        }
        finally {
            executeInPostgres("DROP VIEW " + tableName);
        }
    }

    @Test
    public void testMaterializedView()
    {
        String tableName = "test_stats_materialized_view";
        executeInPostgres("DROP MATERIALIZED VIEW IF EXISTS " + tableName);
        executeInPostgres("" +
                "CREATE MATERIALIZED VIEW " + tableName + " " +
                "AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderpriority', 135000, 5, 0, null, null, null)," +
                            "('comment', 735000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInPostgres("DROP MATERIALIZED VIEW " + tableName);
        }
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        executeInPostgres("" +
                "CREATE TABLE " + tableName + " " +
                "AS SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM orders");
        try {
            gatherStats(tableName);
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, null, null)," +
                            "('case_unquoted_lower', null, 1000, 0, null, null, null)," +
                            "('case_unquoted_mixed', 30000, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, null, null)," +
                            "('case_quoted_lower', null, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', 135000, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            executeInPostgres("DROP TABLE " + tableName);
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

    @Test
    public void testNumericCornerCasesFromPresto()
    {
        try (TestTable table = fromColumns(
                // TODO(https://starburstdata.atlassian.net/browse/PRESTO-4832) we cannot use getQueryRunner()::execute due to current Starburst Remote connector write limitations
                this::executeInRemoteStarburst,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
                        .put("nans_only double", List.of("nan()", "nan()"))
                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .build(),
                "null")) {
            gatherStats(table.getName());
            assertLocalAndRemoteStatistics(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
                            "('mixed_infinities', null, 2, 0, null, null, null)," +
                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "(null, null, null, null, 4, null, null)");
        }
    }

    @Test
    public void testShowStatsWithWhere()
    {
        gatherStats("nation");

        assertLocalAndRemoteStatistics(
                "SHOW STATS FOR nation",
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, null, null), " +
                        "('name', 200.0, 25.0, 0.0, null, null, null), " +
                        "('regionkey', null, 5.0, 0.0, null, null, null), " +
                        "('comment', 1875.0, 25.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 25.0, null, null)");

        // TODO: Fix values after adding filter handling to PostgreSQL connector stats (https://github.com/trinodb/trino/pull/844)
        assertLocalAndRemoteStatistics(
                "SHOW STATS FOR (SELECT * FROM nation WHERE regionkey = 1)",
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, null, null), " +
                        "('name', 200.0, 25.0, 0.0, null, null, null), " +
                        "('regionkey', null, 5.0, 0.0, null, null, null), " +
                        "('comment', 1875.0, 25.0, 0.0, null, null, null), " +
                        "(null, null, null, null, 25.0, null, null)");
    }

    private void assertLocalAndRemoteStatistics(String showStatsQuery, String expectedValues)
    {
        assertQuery(showStatsQuery, expectedValues);
        QueryAssertions.assertQuery(remoteStarburst, remoteSession, showStatsQuery, h2QueryRunner, expectedValues, false, false);
    }

    private void executeInRemoteStarburst(String sql)
    {
        remoteStarburst.execute(remoteSession, sql);
    }

    private void executeInPostgres(String sql)
    {
        inPostgres(handle -> handle.execute(sql));
    }

    private void gatherStats(String tableName)
    {
        inPostgres(handle -> {
            handle.execute("ANALYZE " + tableName);
            for (int i = 0; i < 5; i++) {
                long actualCount = handle.createQuery("SELECT count(*) FROM " + tableName)
                        .mapTo(Long.class)
                        .one();
                long estimatedCount = handle.createQuery(format("SELECT reltuples FROM pg_class WHERE oid = '%s'::regclass::oid", tableName))
                        .mapTo(Long.class)
                        .one();
                if (actualCount == estimatedCount) {
                    return;
                }
                handle.execute("ANALYZE " + tableName);
            }
            throw new IllegalStateException("Stats not gathered"); // for small test tables reltuples should be exact
        });
    }

    private <E extends Exception> void inPostgres(HandleConsumer<E> callback)
            throws E
    {
        Properties properties = new Properties();
        properties.setProperty("currentSchema", "tiny");
        properties.setProperty("user", postgreSqlServer.getUser());
        properties.setProperty("password", postgreSqlServer.getPassword());
        Jdbi.create(postgreSqlServer.getJdbcUrl()
                        // TODO remove `.replaceAll` call after https://github.com/trinodb/trino/pull/7399
                        .replaceAll("currentSchema=\\w+", ""),
                properties)
                .useHandle(callback);
    }
}
