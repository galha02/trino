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
package io.trino.plugin.jdbc;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.NodeManager;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final JdbcModuleProvider moduleProvider;

    public JdbcConnectorFactory(String name, Module module)
    {
        this(name, catalogName -> module);
    }

    /**
     * @deprecated Prefer {@link JdbcConnectorFactory#JdbcConnectorFactory(String, Module)} instead.
     * Notice that {@link io.prestosql.plugin.base.CatalogName} is available in guice context.
     */
    @Deprecated
    public JdbcConnectorFactory(String name, JdbcModuleProvider moduleProvider)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.moduleProvider = requireNonNull(moduleProvider, "moduleProvider is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new JdbcHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                binder -> binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder()),
                new JdbcModule(catalogName),
                moduleProvider.getModule(catalogName));

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(JdbcConnector.class);
    }

    /**
     * @deprecated Prefer {@link JdbcConnectorFactory#JdbcConnectorFactory(String, Module)} instead.
     * Notice that {@link io.prestosql.plugin.base.CatalogName} is available in guice context.
     */
    @Deprecated
    public interface JdbcModuleProvider
    {
        Module getModule(String catalogName);
    }
}
