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
package io.trino.plugin.redshift;

import com.amazon.redshift.util.RedshiftException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class RedshiftUnloadSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RedshiftUnloadSplitSource.class);
    private static final String RESULT_FILES_QUERY = "SELECT rtrim(path) FROM stl_unload_log WHERE query=pg_last_query_id() ORDER BY path";

    private final Connection connection;
    private final CompletableFuture<Boolean> resultSetFuture;
    private final ConnectorSplitSource fallbackSplitSource;

    private boolean finished;

    public RedshiftUnloadSplitSource(ExecutorService executor, Connection connection, PreparedStatement statement, ConnectorSplitSource fallbackSplitSource)
    {
        requireNonNull(executor, "executor is null");
        this.connection = requireNonNull(connection, "connection is null");
        this.fallbackSplitSource = requireNonNull(fallbackSplitSource, "fallbackSplitSource is null");
        requireNonNull(statement, "statement is null");
        resultSetFuture = CompletableFuture.supplyAsync(() -> {
            log.debug("Executing: %s", statement);
            try {
                connection.setReadOnly(false);
                return statement.execute();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return resultSetFuture
                .thenApply(_ -> {
                    ConnectorSplitBatch connectorSplitBatch = new ConnectorSplitBatch(getPaths().stream()
                            .map(path -> (ConnectorSplit) new RedshiftUnloadSplit(path))
                            .toList(), true);
                    finished = true;
                    return connectorSplitBatch;
                })
                .exceptionally(e -> {
                    if (e.getCause() != null && e.getCause().getCause() != null && e.getCause().getCause() instanceof RedshiftException) {
                        try {
                            log.debug("Unload query execution failed. Falling back to using JDBC");
                            return fallbackSplitSource.getNextBatch(maxSize).get();
                        }
                        catch (InterruptedException | ExecutionException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                    throw new RuntimeException(e);
                });
    }

    private List<String> getPaths()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try {
            // TODO is it worth getting transfer_size to tracking/debugging?
            ResultSet results = connection.prepareStatement(RESULT_FILES_QUERY).executeQuery();
            while (results.next()) {
                builder.add(results.getString(1));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return builder.build();
    }

    @Override
    public void close()
    {
        resultSetFuture.cancel(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished || fallbackSplitSource.isFinished();
    }
}
