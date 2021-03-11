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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class JdbcPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcClient jdbcClient;
    private final ExecutorService executor;

    @Inject
    public JdbcPageSourceProvider(JdbcClient jdbcClient, @ForPageSource ExecutorService executor)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        JdbcSplit jdbcSplit = (JdbcSplit) split;
        JdbcTableHandle jdbcTable = (JdbcTableHandle) table;

        // In the current API, the columns (and order) needed by the engine are provided via an argument to this method. Make sure that
        // any columns that were recorded in the table handle match the requested set.
        // If no columns are recorded, it means that applyProjection never got called (e.g., in the case all columns are being used) and all
        // table columns should be returned. TODO: this is something that should be addressed once the getRecordSet API is revamped
        jdbcTable.getColumns()
                .ifPresent(tableColumns -> verify(columns.equals(tableColumns)));

        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((JdbcColumnHandle) handle);
        }

        return new JdbcPageSource(jdbcClient, executor, session, jdbcSplit, jdbcTable, handles.build());
    }
}
