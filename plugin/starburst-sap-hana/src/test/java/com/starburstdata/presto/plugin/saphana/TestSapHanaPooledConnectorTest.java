/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;

public class TestSapHanaPooledConnectorTest
        extends BaseSapHanaConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("connection-pool.enabled", "true")
                        .buildOrThrow(),
                ImmutableMap.of(),
                TpchTable.getTables());
    }
}
