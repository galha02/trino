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
package io.trino.loki;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class LokiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(LokiConnector.class).in(Scopes.SINGLETON);
        binder.bind(LokiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LokiClient.class).in(Scopes.SINGLETON);
        binder.bind(LokiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LokiRecordSetProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(LokiTableFunctionProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(LokiConnectorConfig.class);
    }
}
