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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroBytesDeserializer;
import io.trino.decoder.avro.AvroDeserializer;
import io.trino.decoder.avro.AvroReaderSupplier;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.plugin.kafka.SessionPropertiesProvider;
import io.trino.plugin.kafka.encoder.DispatchingRowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.trino.plugin.kafka.schema.ContentSchemaReader;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;

import javax.inject.Singleton;

import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.kafka.encoder.EncoderModule.encoderFactory;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        install(new ConfluentDecoderModule());
        install(new ConfluentEncoderModule());
        binder.bind(ContentSchemaReader.class).to(AvroConfluentContentSchemaReader.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SchemaProvider.class).addBinding().to(AvroSchemaProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(ConfluentSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TableDescriptionSupplier.class).toProvider(ConfluentSchemaRegistryTableDescriptionSupplier.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, SchemaParser.class).addBinding("AVRO").to(AvroSchemaParser.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SchemaRegistryPropertiesProvider.class);
    }

    @Provides
    @Singleton
    public static SchemaRegistryClient getSchemaRegistryClient(ConfluentSchemaRegistryConfig confluentConfig, Set<SchemaProvider> schemaProviders, Set<SchemaRegistryPropertiesProvider> schemaRegistryPropertiesProviders)
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        schemaRegistryPropertiesProviders.stream()
                .forEach(provider -> builder.putAll(provider.getConfigurationProperties()));
        return new CachedSchemaRegistryClient(
                confluentConfig.getConfluentSchemaRegistryUrls().stream()
                        .map(HostAddress::getHostText)
                        .collect(toImmutableList()),
                confluentConfig.getConfluentSchemaRegistryClientCacheSize(),
                ImmutableList.copyOf(schemaProviders),
                builder.build());
    }

    private static class ConfluentDecoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
            binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
            binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
        }
    }

    private static class ConfluentEncoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            MapBinder<String, RowEncoderFactory> encoderFactoriesByName = encoderFactory(binder);
            encoderFactoriesByName.addBinding(AvroRowEncoder.NAME).toInstance((session, dataSchema, columnHandles) -> {
                throw new TrinoException(NOT_SUPPORTED, "Insert not supported");
            });
            binder.bind(DispatchingRowEncoderFactory.class).in(SINGLETON);
        }
    }
}
