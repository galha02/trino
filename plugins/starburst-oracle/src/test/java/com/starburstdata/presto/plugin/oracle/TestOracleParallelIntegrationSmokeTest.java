/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createStandardUsers;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.createUser;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleParallelIntegrationSmokeTest
        extends BaseStarburstOracleIntegrationSmokeTest
{
    public static final String PARTITIONED_USER = "partitioned_user";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.concurrent.max-splits-per-scan", "17")
                        .build())
                .withSessionModifier(session -> createSession(PARTITIONED_USER, PARTITIONED_USER))
                .withTables(ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION))
                .withCreateUsers(TestOracleParallelIntegrationSmokeTest::createUsers)
                .withProvisionTables(TestOracleParallelIntegrationSmokeTest::partitionTables)
                .withUnlockEnterpriseFeatures(true) // parallelism is license protected
                .build();
    }

    private static void createUsers()
    {
        createStandardUsers();
        createUser(PARTITIONED_USER, OracleTestUsers.KERBERIZED_USER);
        executeInOracle(format("GRANT SELECT ON user_context to %s", PARTITIONED_USER));
    }

    private static void partitionTables()
    {
        executeInOracle(format("ALTER TABLE %s.orders MODIFY PARTITION BY RANGE (orderdate) INTERVAL (NUMTODSINTERVAL(1,'DAY')) (PARTITION before_1900 VALUES LESS THAN (TO_DATE('01-JAN-1900','dd-MON-yyyy')))", PARTITIONED_USER));
        executeInOracle(format("ALTER TABLE %s.nation MODIFY PARTITION BY HASH(name) PARTITIONS 3", PARTITIONED_USER));
    }

    @Test
    @Override
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isNotFullyPushedDown(LimitNode.class);

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isNotFullyPushedDown(LimitNode.class);

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5"))
                // TODO https://github.com/prestosql/presto/issues/5541 .isNotFullyPushedDown ProjectNode.class, LimitNode
                .isNotFullyPushedDown(LimitNode.class);
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isNotFullyPushedDown(ProjectNode.class); // TODO (https://github.com/prestosql/presto/issues/5522)

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3"))
                // TODO https://github.com/prestosql/presto/issues/5541 .isNotFullyPushedDown ProjectNode.class, LimitNode
                .isNotFullyPushedDown(LimitNode.class);
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3"))
                // TODO https://github.com/prestosql/presto/issues/5541 .isNotFullyPushedDown ProjectNode.class, LimitNode
                .isNotFullyPushedDown(LimitNode.class);
    }
}
