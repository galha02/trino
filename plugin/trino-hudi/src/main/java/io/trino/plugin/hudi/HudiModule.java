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

package io.trino.plugin.hudi;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.CachingDirectoryLister;
import io.trino.plugin.hive.DirectoryLister;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HudiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        binder.bind(HudiTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(HudiConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HudiSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HudiTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(HudiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(HudiPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(HudiMetadataFactory.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        newOptionalBinder(binder, ConnectorAccessControl.class).setDefault().to(AllowAllAccessControl.class).in(Scopes.SINGLETON);
    }

    @ForHudiSplitManager
    @Singleton
    @Provides
    public ExecutorService createExecutorService()
    {
        return newCachedThreadPool(daemonThreadsNamed("hudi-split-manager-%d"));
    }
}
