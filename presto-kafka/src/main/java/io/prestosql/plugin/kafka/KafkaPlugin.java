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
package io.prestosql.plugin.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class KafkaPlugin
        implements Plugin
{
    public static final Module DEFAULT_EXTENSION = binder -> {
        binder.install(new KafkaConsumerModule());
    };

    private final Module extension;
    private Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier = Optional.empty();

    public KafkaPlugin()
    {
        this(DEFAULT_EXTENSION);
    }

    public KafkaPlugin(Module extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
    }

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KafkaTopicDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new KafkaConnectorFactory(extension, tableDescriptionSupplier));
    }
}
