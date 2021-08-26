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
package io.trino.plugin.google.sheets;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class SheetsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(SheetsConnector.class).in(Scopes.SINGLETON);
        binder.bind(SheetsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SheetsClient.class).in(Scopes.SINGLETON);
        binder.bind(SheetsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SheetsRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SheetsConfig.class);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(SheetsTable.class));
    }
}
