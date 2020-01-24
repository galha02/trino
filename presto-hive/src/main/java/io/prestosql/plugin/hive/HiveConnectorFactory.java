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
package io.prestosql.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Class<? extends Module> module;

    public HiveConnectorFactory(String name)
    {
        this(name, EmptyModule.class);
    }

    public HiveConnectorFactory(String name, Class<? extends Module> module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = context.duplicatePluginClassLoader();
        try {
            Object moduleInstance = classLoader.loadClass(module.getName()).getConstructor().newInstance();
            Class<?> moduleClass = classLoader.loadClass(Module.class.getName());
            return (Connector) classLoader.loadClass(InternalHiveConnectorFactory.class.getName())
                    .getMethod("createConnector", String.class, Map.class, ConnectorContext.class, moduleClass)
                    .invoke(null, catalogName, config, context, moduleInstance);
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throwIfUnchecked(targetException);
            throw new RuntimeException(targetException);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public static class EmptyModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}
    }
}
