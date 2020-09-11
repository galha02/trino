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
package io.prestosql.pinot;

import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PinotConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final PinotMetadata metadata;
    private final PinotSplitManager splitManager;
    private final PinotPageSourceProvider pageSourceProvider;
    private final PinotNodePartitioningProvider partitioningProvider;
    private final PinotSessionProperties sessionProperties;

    @Inject
    public PinotConnector(
            LifeCycleManager lifeCycleManager,
            PinotMetadata metadata,
            PinotSplitManager splitManager,
            PinotPageSourceProvider pageSourceProvider,
            PinotNodePartitioningProvider partitioningProvider,
            PinotSessionProperties pinotSessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.partitioningProvider = requireNonNull(partitioningProvider, "partitioningProvider is null");
        this.sessionProperties = requireNonNull(pinotSessionProperties, "sessionProperties is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return PinotTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
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
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.copyOf(sessionProperties.getSessionProperties());
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
