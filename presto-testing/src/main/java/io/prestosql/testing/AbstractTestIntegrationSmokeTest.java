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
package io.prestosql.testing;

import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.QueryAssertions.assertContains;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

/**
 * Generic test for connectors exercising connector's read capabilities.
 *
 * @see AbstractTestDistributedQueries
 */
public abstract class AbstractTestIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    /**
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testAggregateSingleColumn()
    {
        assertQuery("SELECT SUM(orderkey) FROM orders");
        assertQuery("SELECT SUM(totalprice) FROM orders");
        assertQuery("SELECT MAX(comment) FROM orders");
    }

    @Test
    public void testColumnsInReverseOrder()
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM orders");
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
    }

    @Test
    public void testExactPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey = 10");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey = 32", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey = 32", "VALUES (1301)");
    }

    @Test
    public void testInListPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey IN (10, 11, 20, 21)");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey IN (7, 10, 32, 33)", "VALUES (392, 7), (1301, 32), (670, 33)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey IN (7, 10, 32, 33)", "VALUES (392), (1301), (670)");
    }

    @Test
    public void testIsNullPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey IS NULL");
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey = 10 OR orderkey IS NULL");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey = 32 OR orderkey IS NULL", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey = 32 OR orderkey IS NULL", "VALUES (1301)");
    }

    @Test
    public void testLikePredicate()
    {
        // filtered column is not selected
        assertQuery("SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is selected
        assertQuery("SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is not selected
        assertQuery("SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L__'");

        // filtered column is selected
        assertQuery("SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L__'");
    }

    @Test
    public void testLimit()
    {
        assertEquals(computeActual("SELECT * FROM orders LIMIT 10").getRowCount(), 10);
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        // List columns explicitly. Some connectors do not maintain column ordering.
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testRangePredicate()
    {
        // List columns explicitly. Some connectors do not maintain column ordering.
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testConcurrentScans()
    {
        String unionMultipleTimes = join(" UNION ALL ", nCopies(25, "SELECT * FROM orders"));
        assertQuery("SELECT sum(if(rand() >= 0, orderkey)) FROM (" + unionMultipleTimes + ")", "VALUES 11246812500");
    }

    @Test
    public void testSelectAll()
    {
        assertQuery("SELECT * FROM orders");
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toTestTypes();

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row(getQueryRunner().getDefaultSession().getSchema().orElse("tpch"));

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult actualTables = computeActual("SHOW TABLES").toTestTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row("orders")
                .build();
        assertContains(actualTables, expectedTables);
    }

    @Test
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders WHERE orderkey < 0");
    }

    protected void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));

        // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
        // assertTrue(value.contains("Cost: "), format("Expected output to contain \"Cost: \", but it is %s", value));
    }

    @Test
    public void testTableSampleSystem()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult randomSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (50)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
        assertTrue(all.getMaterializedRows().size() >= randomSample.getMaterializedRows().size());
    }

    @Test
    public void testTableSampleWithFiltering()
    {
        MaterializedResult emptySample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (99) WHERE orderkey BETWEEN 0 AND 0");
        MaterializedResult halfSample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (50) WHERE orderkey BETWEEN 0 AND 9999999999");
        MaterializedResult all = computeActual("SELECT orderkey, orderdate FROM orders");

        assertEquals(emptySample.getMaterializedRows().size(), 0);
        // Assertions need to be loose here because SYSTEM sampling random selects data on split boundaries. In this case either all the data will be selected, or
        // none of it. Sampling with a 100% ratio is ignored, so that also cannot be used to guarantee results.
        assertTrue(all.getMaterializedRows().size() >= halfSample.getMaterializedRows().size());
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll("^.", "_");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders'",
                "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'",
                ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }
}
