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
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class SapHanaQueryRunner
{
    public static final String GRANTED_USER = "alice";
    public static final String NON_GRANTED_USER = "bob";

    private SapHanaQueryRunner() {}

    public static DistributedQueryRunner createSapHanaQueryRunner(
            TestingSapHanaServer server,
            Map<String, String> connectorProperties,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = StarburstDistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            server.executeWithRetry("CREATE SCHEMA tpch");
            server.executeWithRetry("CREATE USER " + GRANTED_USER);
            server.executeWithRetry("CREATE USER " + NON_GRANTED_USER);
            server.executeWithRetry("GRANT ALL PRIVILEGES ON SCHEMA tpch TO " + GRANTED_USER);

            queryRunner.installPlugin(new TestingSapHanaPlugin());
            queryRunner.createCatalog("saphana", "sap-hana", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession()
    {
        return createSession(GRANTED_USER);
    }

    public static Session createSession(String user)
    {
        return testSessionBuilder()
                .setCatalog("saphana")
                .setSchema("tpch")
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        TestingSapHanaServer sapHanaServer = TestingSapHanaServer.create();
        DistributedQueryRunner queryRunner = createSapHanaQueryRunner(
                sapHanaServer,
                ImmutableMap.<String, String>builder()
                        .put("connection-url", sapHanaServer.getJdbcUrl())
                        .build(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());

        Logger log = Logger.get(SapHanaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
