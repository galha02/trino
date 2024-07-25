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
package io.trino.plugin.deltalake.metastore.glue.v1;

import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.glue.DeltaLakeGlueMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hive.metastore.glue.v1.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.v1.GlueMetastoreModule;

import java.util.function.Predicate;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DeltaLakeGlueV1MetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DeltaLakeGlueMetastoreConfig.class);

        newOptionalBinder(binder, Key.get(new TypeLiteral<Predicate<Table>>() {}, ForGlueHiveMetastore.class))
                .setBinding().toProvider(DeltaLakeGlueMetastoreTableFilterProvider.class);

        install(new GlueMetastoreModule());
        binder.bind(GlueMetastoreStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GlueMetastoreStats.class).withGeneratedName();
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeGlueV1MetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(true);
        // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html#Glue-Type-TableInput-Parameters as of this writing)
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(512000);
    }
}
