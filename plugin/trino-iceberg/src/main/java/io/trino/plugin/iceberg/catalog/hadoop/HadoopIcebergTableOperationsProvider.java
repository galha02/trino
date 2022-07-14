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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.inject.Inject;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

public class HadoopIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final FileIoProvider fileIoProvider;

    @Inject
    public HadoopIcebergTableOperationsProvider(FileIoProvider fileIoProvider)
    {
        this.fileIoProvider = fileIoProvider;
    }

    @Override
    public IcebergTableOperations createTableOperations(TrinoCatalog catalog, ConnectorSession session, String database, String table, Optional<String> owner, Optional<String> location)
    {
        return new HadoopIcebergTableOperations(
                fileIoProvider.createFileIo(new HdfsContext(session),
                session.getQueryId()),
                session,
                database,
                table,
                owner,
                location,
                (TrinoHadoopCatalog) catalog);
    }
}
