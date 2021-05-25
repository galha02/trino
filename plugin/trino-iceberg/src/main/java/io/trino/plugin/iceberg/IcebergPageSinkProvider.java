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
package io.trino.plugin.iceberg;

import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;

    @Inject
    public IcebergPageSinkProvider(
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(session, (IcebergWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle)
    {
        HdfsContext hdfsContext = new HdfsContext(session, tableHandle.getSchemaName(), tableHandle.getTableName());
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, tableHandle.getPartitionSpecAsJson());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                tableHandle.getOutputPath(),
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                tableHandle.getInputColumns(),
                jsonCodec,
                session,
                tableHandle.getFileFormat());
    }
}
