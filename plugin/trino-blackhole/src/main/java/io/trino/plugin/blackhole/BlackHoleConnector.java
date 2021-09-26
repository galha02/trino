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
package io.trino.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class BlackHoleConnector
        implements Connector
{
    public static final String SPLIT_COUNT_PROPERTY = "split_count";
    public static final String PAGES_PER_SPLIT_PROPERTY = "pages_per_split";
    public static final String ROWS_PER_PAGE_PROPERTY = "rows_per_page";
    public static final String FIELD_LENGTH_PROPERTY = "field_length";
    public static final String DISTRIBUTED_ON = "distributed_on";
    public static final String PAGE_PROCESSING_DELAY = "page_processing_delay";

    private final BlackHoleMetadata metadata;
    private final BlackHoleSplitManager splitManager;
    private final BlackHolePageSourceProvider pageSourceProvider;
    private final BlackHolePageSinkProvider pageSinkProvider;
    private final BlackHoleNodePartitioningProvider partitioningProvider;
    private final TypeManager typeManager;
    private final ExecutorService executorService;

    public BlackHoleConnector(
            BlackHoleMetadata metadata,
            BlackHoleSplitManager splitManager,
            BlackHolePageSourceProvider pageSourceProvider,
            BlackHolePageSinkProvider pageSinkProvider,
            BlackHoleNodePartitioningProvider partitioningProvider,
            TypeManager typeManager,
            ExecutorService executorService)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
        this.partitioningProvider = partitioningProvider;
        this.typeManager = typeManager;
        this.executorService = executorService;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return BlackHoleTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                integerProperty(
                        SPLIT_COUNT_PROPERTY,
                        "Number of splits generated by this table",
                        0,
                        false),
                integerProperty(
                        PAGES_PER_SPLIT_PROPERTY,
                        "Number of pages per each split generated by this table",
                        0,
                        false),
                integerProperty(
                        ROWS_PER_PAGE_PROPERTY,
                        "Number of rows per each page generated by this table",
                        0,
                        false),
                integerProperty(
                        FIELD_LENGTH_PROPERTY,
                        "Overwrite default length (16) of variable length columns, such as VARCHAR or VARBINARY",
                        16,
                        false),
                new PropertyMetadata<>(
                        DISTRIBUTED_ON,
                        "Distribution columns",
                        typeManager.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.typeParameter(createUnboundedVarcharType().getTypeSignature()))),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((List<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toList())),
                        List.class::cast),
                durationProperty(
                        PAGE_PROCESSING_DELAY,
                        "Sleep duration before processing each page",
                        new Duration(0, SECONDS),
                        false));
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return partitioningProvider;
    }

    @Override
    public void shutdown()
    {
        executorService.shutdownNow();
    }
}
