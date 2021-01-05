/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocal;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PrestoConnectorAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD_PASS_THROUGH,
                new PasswordPassthroughModule()));

        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD && !config.isImpersonationEnabled(),
                new PasswordModule()));

        install(installModuleIf(
                PrestoConnectorConfig.class,
                config -> config.getPrestoAuthenticationType() == PASSWORD && config.isImpersonationEnabled(),
                new PasswordWithImpersonationModule()));
    }

    private static class PasswordPassthroughModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, PrestoConnectorConfig prestoConnectorConfig)
        {
            checkState(
                    !prestoConnectorConfig.isImpersonationEnabled(),
                    "User impersonation cannot be used along with PASSWORD_PASS_THROUGH authentication");

            return new DriverConnectionFactory(new TrinoDriver(), config, new PassThroughCredentialProvider());
        }
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(PrestoConnectorCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(new TrinoDriver(), config, credentialProvider);
        }
    }

    private static class PasswordWithImpersonationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new AuthToLocalModule());
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(PrestoConnectorCredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, AuthToLocal authToLocal, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(
                    new TrinoDriver(),
                    config.getConnectionUrl(),
                    new Properties(),
                    new PrestoConnectorImpersonatingCredentialPropertiesProvider(credentialProvider, authToLocal));
        }
    }
}
