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
package io.trino.plugin.redis;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Creates Redis Connectors based off catalogName and specific configuration.
 */
public class RedisConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier;

    RedisConnectorFactory(Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "redis";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new RedisHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new RedisConnectorModule(),
                binder -> {
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());

                    if (tableDescriptionSupplier.isPresent()) {
                        binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                    }
                    else {
                        binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() {})
                                .to(RedisTableDescriptionSupplier.class)
                                .in(Scopes.SINGLETON);
                    }
                });

        Injector injector = app.strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(RedisConnector.class);
    }
}
