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
package io.trino.plugin.bigquery;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BigQueryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final BigQueryWriteClientFactory clientFactory;

    @Inject
    public BigQueryPageSinkProvider(BigQueryWriteClientFactory clientFactory)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        BigQueryOutputTableHandle handle = (BigQueryOutputTableHandle) outputTableHandle;
        return new BigQueryPageSink(
                clientFactory.create(session),
                handle.remoteTableName(),
                handle.columnNames(),
                handle.columnTypes(),
                pageSinkId,
                handle.temporaryTableName(),
                handle.pageSinkIdColumnName());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        BigQueryInsertTableHandle handle = (BigQueryInsertTableHandle) insertTableHandle;
        return new BigQueryPageSink(
                clientFactory.create(session),
                handle.remoteTableName(),
                handle.columnNames(),
                handle.columnTypes(),
                pageSinkId,
                Optional.of(handle.temporaryTableName()),
                Optional.of(handle.pageSinkIdColumnName()));
    }
}
