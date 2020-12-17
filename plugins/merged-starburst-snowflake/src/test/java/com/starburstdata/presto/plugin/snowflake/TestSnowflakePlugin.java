/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSnowflakePlugin
{
    @Test
    public void testLicenseRequired()
    {
        Plugin plugin = new SnowflakePlugin();

        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertThat(connectorFactories).isNotEmpty();

        for (ConnectorFactory factory : connectorFactories) {
            assertThatThrownBy(() -> factory.create("test", ImmutableMap.of("connection-url", "jdbc:snowflake:test"), new TestingConnectorContext()))
                    .isInstanceOf(RuntimeException.class)
                    .hasToString("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: snowflake");
        }
    }

    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TestingSnowflakePlugin();
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(plugin.getConnectorFactories());
        assertEquals(connectorFactories.size(), 2);

        connectorFactories.get(0).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:snowflake:test",
                        "snowflake.role", "test",
                        "snowflake.database", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
        connectorFactories.get(1).create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:snowflake:test",
                        "snowflake.impersonation-type", "ROLE",
                        "snowflake.database", "test",
                        "snowflake.stage-schema", "test",
                        "snowflake.warehouse", "test"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testLicenseRequiredForImpersonation()
    {
        Plugin plugin = new SnowflakePlugin();
        ConnectorFactory factory = stream(plugin.getConnectorFactories())
                .filter(connectorFactory -> connectorFactory.getName().equals("snowflake-jdbc"))
                .collect(toOptional())
                .orElseThrow();

        for (SnowflakeImpersonationType impersonationType : SnowflakeImpersonationType.values()) {
            if (impersonationType == SnowflakeImpersonationType.NONE) {
                continue;
            }
            assertThatThrownBy(() -> factory.create(
                    "test",
                    ImmutableMap.of(
                            "connection-url", "jdbc:snowflake:test",
                            "snowflake.role", "test",
                            "snowflake.database", "test",
                            "snowflake.warehouse", "test",
                            "snowflake.impersonation-type", impersonationType.name()),
                    new TestingConnectorContext()))
                    .describedAs("create failure for " + impersonationType)
                    .isInstanceOf(RuntimeException.class)
                    // We expect 'snowflake' not 'jdbc-impersonation' -- the test exists just in case we base our Snowflake connector on open source in the future
                    .hasStackTraceContaining("com.starburstdata.presto.license.PrestoLicenseException: Valid license required to use the feature: snowflake");
        }
    }
}
