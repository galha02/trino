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
package io.trino.plugin.deltalake.kernel.clients;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.defaults.client.DefaultExpressionHandler;
import io.delta.kernel.defaults.client.DefaultFileSystemClient;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;

public class KernelTableClient
        implements TableClient
{
    private final Configuration configuration;
    private final TrinoFileSystem fileSystem;
    private final TypeManager typeManager;

    public KernelTableClient(
            Configuration configuration,
            TrinoFileSystem fileSystem,
            TypeManager typeManager)
    {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
        this.typeManager = typeManager;
    }

    @Override
    public ExpressionHandler getExpressionHandler()
    {
        return new DefaultExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler()
    {
        return new KernelJsonHandler(configuration);
    }

    @Override
    public FileSystemClient getFileSystemClient()
    {
        return new DefaultFileSystemClient(configuration);
    }

    @Override
    public ParquetHandler getParquetHandler()
    {
        return new KernelParquetHandler(
                configuration,
                fileSystem,
                typeManager);
    }
}
