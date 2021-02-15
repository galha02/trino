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
package io.trino.plugin.kudu;

import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class KuduNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final KuduClientSession clientSession;

    @Inject
    public KuduNodePartitioningProvider(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        KuduPartitioningHandle handle = (KuduPartitioningHandle) partitioningHandle;
        return IntStream.range(0, handle.getBucketCount())
                .mapToObj(KuduPartitionHandle::new)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        KuduPartitioningHandle handle = (KuduPartitioningHandle) partitioningHandle;
        return createBucketNodeMap(handle.getBucketCount());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((KuduSplit) value).getBucketNumber();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        KuduPartitioningHandle handle = (KuduPartitioningHandle) partitioningHandle;
        return new KuduBucketFunction(
                handle.getBucketColumnIndexes(),
                partitionChannelTypes,
                clientSession.openTable(new SchemaTableName(handle.getSchema(), handle.getTable())));
    }
}
