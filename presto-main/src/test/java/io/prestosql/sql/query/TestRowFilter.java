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
package io.prestosql.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.TestingAccessControlManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestRowFilter
{
    private static final String CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";
    private static final String RUN_AS_USER = "run-as-user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @BeforeClass
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey, name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId()), new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false);

        MockConnectorFactory mock = MockConnectorFactory.builder()
                .withGetViews((s, prefix) -> ImmutableMap.<SchemaTableName, ConnectorViewDefinition>builder()
                        .put(new SchemaTableName("default", "nation_view"), view)
                        .build())
                .build();

        runner.createCatalog(MOCK_CATALOG, mock, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleFilter()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey < 10"));
            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '7'");
        });

        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "NULL"));
            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '0'");
        });
    }

    @Test
    public void testMultipleFilters()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey < 10"));

            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey > 5"));

            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '2'");
        });
    }

    @Test
    public void testCorrelatedSubquery()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "EXISTS (SELECT 1 FROM nation WHERE nationkey = orderkey)"));
            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '7'");
        });
    }

    @Test
    public void testView()
    {
        // filter on the underlying table for view owner when running query as different user
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "nation"),
                    VIEW_OWNER,
                    new ViewExpression(VIEW_OWNER, Optional.empty(), Optional.empty(), "nationkey = 1"));

            Session session = Session.builder(SESSION)
                    .setIdentity(Identity.forUser(RUN_AS_USER).build())
                    .build();

            assertions.assertQuery(session, "SELECT name FROM mock.default.nation_view", "VALUES CAST('ARGENTINA' AS VARCHAR(25))");
        });

        // filter on the underlying table for view owner when running as themselves
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "nation"),
                    VIEW_OWNER,
                    new ViewExpression(VIEW_OWNER, Optional.of(CATALOG), Optional.of("tiny"), "nationkey = 1"));

            Session session = Session.builder(SESSION)
                    .setIdentity(Identity.forUser(VIEW_OWNER).build())
                    .build();

            assertions.assertQuery(session, "SELECT name FROM mock.default.nation_view", "VALUES CAST('ARGENTINA' AS VARCHAR(25))");
        });

        // filter on the underlying table for user running the query (different from view owner) should not be applied
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "nation"),
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "nationkey = 1"));

            Session session = Session.builder(SESSION)
                    .setIdentity(Identity.forUser(RUN_AS_USER).build())
                    .build();

            assertions.assertQuery(session, "SELECT count(*) FROM mock.default.nation_view", "VALUES BIGINT '25'");
        });
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey = 1"));
            assertions.assertQuery("WITH t AS (SELECT count(*) FROM orders) SELECT * FROM t", "VALUES BIGINT '1'");
        });
    }

    @Test
    public void testOtherSchema()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("sf1"), "(SELECT count(*) FROM customer) = 150000")); // Filter is TRUE only if evaluating against sf1.customer
            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '15000'");
        });
    }

    @Test
    public void testDifferentIdentity()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 1"));

            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

            assertions.assertQuery("SELECT count(*) FROM orders", "VALUES BIGINT '1'");
        });
    }

    @Test
    public void testRecursion()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

            assertions.assertFails("SELECT count(*) FROM orders", ".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
        });

        // different reference style to same table
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT local.tiny.orderkey FROM orders)"));
            assertions.assertFails("SELECT count(*) FROM orders", ".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
        });

        // mutual recursion
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

            assertions.assertFails("SELECT count(*) FROM orders", ".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
        });
    }

    @Test
    public void testLimitedScope()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "customer"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 1"));
            assertions.assertFails(
                    "SELECT (SELECT min(name) FROM customer WHERE customer.custkey = orders.custkey) FROM orders",
                    "\\Qline 1:31: Invalid row filter for 'local.tiny.customer': Column 'orderkey' cannot be resolved\\E");
        });
    }

    @Test
    public void testSqlInjection()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "nation"),
                    USER,
                    new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "regionkey IN (SELECT regionkey FROM region WHERE name = 'ASIA')"));
            assertions.assertQuery(
                    "WITH region(regionkey, name) AS (VALUES (0, 'ASIA'), (1, 'ASIA'), (2, 'ASIA'), (3, 'ASIA'), (4, 'ASIA'))" +
                            "SELECT name FROM nation ORDER BY name LIMIT 1",
                    "VALUES CAST('CHINA' AS VARCHAR(25))"); // if sql-injection would work then query would return ALGERIA
        });
    }

    @Test
    public void testInvalidFilter()
    {
        // parse error
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "$$$"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:22: Invalid row filter for 'local.tiny.orders': mismatched input '$'. Expecting: <expression>\\E");
        });

        // unknown column
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "unknown_column"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:22: Invalid row filter for 'local.tiny.orders': Column 'unknown_column' cannot be resolved\\E");
        });

        // invalid type
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "1"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:22: Expected row filter for 'local.tiny.orders' to be of type BOOLEAN, but was integer\\E");
        });

        // aggregation
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "count(*) > 0"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:10: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [count(*)]\\E");
        });

        // window function
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "row_number() OVER () > 0"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:22: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]\\E");
        });

        // window function
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "grouping(orderkey) = 0"));

            assertions.assertFails("SELECT count(*) FROM orders", "\\Qline 1:20: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]\\E");
        });
    }

    @Test
    public void testShowStats()
    {
        assertions.executeExclusively(() -> {
            accessControl.reset();
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 0"));

            assertions.assertFails("SHOW STATS FOR (SELECT * FROM tiny.orders)", "\\QSHOW STATS is not supported for a table with row filtering");
        });
    }
}
