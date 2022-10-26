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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.trino.tpch.TpchTable.NATION;

public class TestSqlServerLicenseProtection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        return createStarburstSqlServerQueryRunner(sqlServer, false, ImmutableMap.of(), ImmutableList.of(NATION));
    }

    @Test
    public void testLicenseProtectionOfParallelismViaSessionProperty()
    {
        Session noParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("sqlserver", "parallel_connections_count", "1")
                .build();

        Session partitionsParallelismSession = Session.builder(getSession())
                .setCatalogSessionProperty("sqlserver", "parallel_connections_count", "4")
                .build();

        assertQuery(noParallelismSession, "SELECT * FROM nation");
        assertQueryFails(partitionsParallelismSession, "SELECT * FROM nation", "Valid license required to use the feature: sqlserver-extensions");
    }
}
