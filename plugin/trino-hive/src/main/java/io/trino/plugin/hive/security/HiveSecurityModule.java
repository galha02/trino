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
package io.trino.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationModule.installModules;
import static java.util.Objects.requireNonNull;

public class HiveSecurityModule
        extends AbstractConfigurationAwareModule
{
    public static final String LEGACY = "legacy";
    public static final String FILE = "file";
    public static final String READ_ONLY = "read-only";
    public static final String SQL_STANDARD = "sql-standard";
    public static final String ALLOW_ALL = "allow-all";

    private final String catalogName;

    public HiveSecurityModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        bindSecurityModule(
                LEGACY,
                installModules(
                        new LegacySecurityModule(),
                        new StaticAccessControlMetadataModule()));
        bindSecurityModule(
                FILE,
                installModules(
                        new FileBasedAccessControlModule(catalogName),
                        new StaticAccessControlMetadataModule()));
        bindSecurityModule(
                READ_ONLY,
                installModules(
                        new ReadOnlySecurityModule(),
                        new StaticAccessControlMetadataModule()));
        bindSecurityModule(SQL_STANDARD, new SqlStandardSecurityModule());
        bindSecurityModule(ALLOW_ALL, new AllowAllSecurityModule());
    }

    private void bindSecurityModule(String name, Module module)
    {
        install(conditionalModule(
                SecurityConfig.class,
                security -> name.equalsIgnoreCase(security.getSecuritySystem()),
                module));
    }

    private static class StaticAccessControlMetadataModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AccessControlMetadataFactory.class).toInstance((metastore) -> new AccessControlMetadata() {});
        }
    }
}
