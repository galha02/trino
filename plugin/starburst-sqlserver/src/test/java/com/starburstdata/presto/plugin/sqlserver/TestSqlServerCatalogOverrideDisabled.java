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
import io.trino.Session;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.sqlserver.SqlServerSessionProperties.OVERRIDE_CATALOG;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.CATALOG;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.function.Function.identity;

public class TestSqlServerCatalogOverrideDisabled
        extends AbstractTestQueryFramework
{
    private static final Session OVERRIDDEN_SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TEST_SCHEMA)
            .setIdentity(Identity.ofUser(ALICE_USER))
            .setCatalogSessionProperty(CATALOG, OVERRIDE_CATALOG, "master")
            .build();

    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        return createStarburstSqlServerQueryRunner(
                sqlServer,
                identity(),
                true,
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testDisabledByConfig()
    {
        assertQueryFails(
                OVERRIDDEN_SESSION,
                "SELECT COUNT(*) FROM non_existent",
                "Catalog override is disabled");
    }
}
