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
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcPageSinkProvider;
import io.prestosql.plugin.jdbc.JdbcTransactionHandle;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.immutableEnumSet;
import static io.prestosql.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedConnector
        implements Connector
{
    private static final Logger log = Logger.get(SnowflakeDistributedConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final SnowflakeMetadataFactory metadataFactory;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final JdbcPageSinkProvider jdbcPageSinkProvider;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Set<Procedure> procedures;
    private final Set<SessionPropertiesProvider> sessionProperties;

    private final ConcurrentMap<ConnectorTransactionHandle, JdbcMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public SnowflakeDistributedConnector(
            LifeCycleManager lifeCycleManager,
            SnowflakeMetadataFactory metadataFactory,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            JdbcPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<SessionPropertiesProvider> sessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.jdbcPageSinkProvider = requireNonNull(jdbcPageSinkProvider, "jdbcPageSinkProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        JdbcTransactionHandle transaction = new JdbcTransactionHandle();
        transactions.put(transaction, metadataFactory.create());
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        JdbcMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return jdbcPageSinkProvider;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        ImmutableList.Builder builder = ImmutableList.<PropertyMetadata<?>>builder();
        this.sessionProperties.forEach(sessionPropertiesProvider -> builder.addAll(sessionPropertiesProvider.getSessionProperties()));
        return builder.build();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }
}
