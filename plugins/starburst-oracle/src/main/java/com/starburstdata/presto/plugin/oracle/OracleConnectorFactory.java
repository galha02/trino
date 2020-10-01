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

import com.starburstdata.presto.license.LicenseModule;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class OracleConnectorFactory
        extends JdbcConnectorFactory
{
    public OracleConnectorFactory()
    {
        super("oracle", catalogName -> {
            return combine(new LicenseModule(), new OracleClientModule(catalogName));
        });
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new OracleHandleResolver();
    }
}
