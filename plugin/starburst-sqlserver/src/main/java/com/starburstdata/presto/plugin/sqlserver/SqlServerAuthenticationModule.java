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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.presto.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMappingModule;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.auth.PasswordPassThroughModule;
import com.starburstdata.presto.plugin.sqlserver.CatalogOverridingModule.ForCatalogOverriding;
import com.starburstdata.presto.plugin.toolkit.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import static com.google.inject.Scopes.SINGLETON;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerConfig.SqlServerAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class SqlServerAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));

        install(conditionalModule(
                StarburstSqlServerConfig.class,
                config -> config.getAuthenticationType() == PASSWORD_PASS_THROUGH,
                new SqlServerPasswordPassThroughModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            install(conditionalModule(
                    StarburstSqlServerConfig.class,
                    StarburstSqlServerConfig::isImpersonationEnabled,
                    new ImpersonationModule(),
                    noImpersonationModuleWithCredentialProvider()));
        }

        @Provides
        @Singleton
        @ForCatalogOverriding
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, credentialProvider);
        }
    }

    private static class SqlServerPasswordPassThroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new PasswordPassThroughModule<>(StarburstSqlServerConfig.class, StarburstSqlServerConfig::isImpersonationEnabled));
            binder.bind(ConnectionFactory.class)
                    .annotatedWith(ForBaseJdbc.class)
                    .to(Key.get(ConnectionFactory.class, ForImpersonation.class))
                    .in(SINGLETON);
        }

        @Provides
        @Singleton
        @ForCatalogOverriding
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return createBasicConnectionFactory(config, credentialProvider);
        }
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.install(new AuthenticationBasedIdentityCacheMappingModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(SINGLETON);
        }
    }

    private static ConnectionFactory createBasicConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        return new DriverConnectionFactory(new SQLServerDriver(), config, credentialProvider);
    }
}
