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
package io.trino.filesystem.hdfs;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.hdfs.TrinoHdfsFileSystemStats;

import static com.google.inject.Scopes.SINGLETON;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HdfsFileSystemModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HdfsFileSystemFactory.class).in(SINGLETON);
        binder.bind(TrinoHdfsFileSystemStats.class).in(SINGLETON);
        newExporter(binder).export(TrinoHdfsFileSystemStats.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public TrinoFileSystemFactory createFileSystemFactory(Tracer tracer, HdfsFileSystemFactory delegate)
    {
        return new TracingFileSystemFactory(tracer, delegate);
    }
}
