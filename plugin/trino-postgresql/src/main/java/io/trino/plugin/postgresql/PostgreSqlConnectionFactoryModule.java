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
package io.trino.plugin.postgresql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import org.postgresql.Driver;

import java.util.Properties;

import static org.postgresql.PGProperty.REWRITE_BATCHED_INSERTS;

public class PostgreSqlConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder) {}

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, PostgreSqlConfig postgreSqlConfig, OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = getConnectionProperties(postgreSqlConfig);
        connectionProperties.put(REWRITE_BATCHED_INSERTS.getName(), "true");
        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    public static Properties getConnectionProperties(PostgreSqlConfig postgreSqlConfig)
    {
        Properties connectionProperties = new Properties();
        if (postgreSqlConfig.getJdbcOptions() != null) {
            connectionProperties.setProperty("options", postgreSqlConfig.getJdbcOptions());
        }
        return connectionProperties;
    }
}
