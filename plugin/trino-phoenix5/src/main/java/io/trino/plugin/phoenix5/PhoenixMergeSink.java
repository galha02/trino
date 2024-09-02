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
package io.trino.plugin.phoenix5;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SinkSqlProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY_COLUMN_HANDLE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.phoenix.util.SchemaUtil.getEscapedArgument;

public class PhoenixMergeSink
        implements ConnectorMergeSink
{
    private final RemoteTableName remoteTableName;
    private final boolean hasRowKey;
    private final int columnCount;
    private final List<String> mergeRowIdFieldNames;

    private final ConnectorPageSink insertSink;
    private final ConnectorPageSink updateSink;
    private final ConnectorPageSink deleteSink;

    public PhoenixMergeSink(PhoenixClient phoenixClient, RemoteQueryModifier remoteQueryModifier, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        PhoenixMergeTableHandle phoenixMergeTableHandle = (PhoenixMergeTableHandle) mergeHandle;
        PhoenixOutputTableHandle phoenixOutputTableHandle = phoenixMergeTableHandle.phoenixOutputTableHandle();
        this.remoteTableName = phoenixOutputTableHandle.getRemoteTableName();
        this.hasRowKey = phoenixOutputTableHandle.rowkeyColumn().isPresent();
        this.columnCount = phoenixOutputTableHandle.getColumnNames().size();

        this.insertSink = new JdbcPageSink(session, phoenixOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, JdbcClient::buildInsertSql);
        this.updateSink = createUpdateSink(session, phoenixOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier);

        ImmutableList.Builder<String> mergeRowIdFieldNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> mergeRowIdFieldTypesBuilder = ImmutableList.builder();
        RowType mergeRowIdColumnType = (RowType) phoenixMergeTableHandle.mergeRowIdColumnHandle().getColumnType();
        for (RowType.Field field : mergeRowIdColumnType.getFields()) {
            checkArgument(field.getName().isPresent(), "Merge row id column field must have name");
            mergeRowIdFieldNamesBuilder.add(getEscapedArgument(field.getName().get()));
            mergeRowIdFieldTypesBuilder.add(field.getType());
        }
        this.mergeRowIdFieldNames = mergeRowIdFieldNamesBuilder.build();
        this.deleteSink = createDeleteSink(session, mergeRowIdFieldTypesBuilder.build(), phoenixClient, pageSinkId, remoteQueryModifier);
    }

    private ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            PhoenixOutputTableHandle phoenixOutputTableHandle,
            PhoenixClient phoenixClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        columnNamesBuilder.addAll(phoenixOutputTableHandle.getColumnNames());
        columnTypesBuilder.addAll(phoenixOutputTableHandle.getColumnTypes());
        if (hasRowKey) {
            columnNamesBuilder.add(ROWKEY);
            columnTypesBuilder.add(ROWKEY_COLUMN_HANDLE.getColumnType());
        }

        PhoenixOutputTableHandle updateOutputTableHandle = new PhoenixOutputTableHandle(
                remoteTableName,
                columnNamesBuilder.build(),
                columnTypesBuilder.build(),
                Optional.empty(),
                Optional.empty());
        return new JdbcPageSink(session, updateOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, JdbcClient::buildInsertSql);
    }

    private ConnectorPageSink createDeleteSink(
            ConnectorSession session,
            List<Type> mergeRowIdFieldTypes,
            PhoenixClient phoenixClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        checkArgument(mergeRowIdFieldNames.size() == mergeRowIdFieldTypes.size(), "Wrong merge row column, columns and types size not match");
        JdbcOutputTableHandle deleteOutputTableHandle = new PhoenixOutputTableHandle(
                remoteTableName,
                mergeRowIdFieldNames,
                mergeRowIdFieldTypes,
                Optional.empty(),
                Optional.empty());

        return new JdbcPageSink(session, deleteOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, deleteSqlProvider());
    }

    private SinkSqlProvider deleteSqlProvider()
    {
        List<String> conjuncts = mergeRowIdFieldNames.stream()
                .map(name -> name + " = ? ")
                .collect(toImmutableList());
        checkArgument(!conjuncts.isEmpty(), "Merge row id fields should not empty");
        String whereCondition = Joiner.on(" AND ").join(conjuncts);

        return (_, _, _) -> format("DELETE FROM %s.%s WHERE %s", remoteTableName.getSchemaName().orElseThrow(), remoteTableName.getTableName(), whereCondition);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        int positionCount = page.getPositionCount();
        Block operationBlock = page.getBlock(columnCount);

        int[] dataChannel = IntStream.range(0, columnCount).toArray();
        Page dataPage = page.getColumns(dataChannel);

        int[] insertPositions = new int[positionCount];
        int insertPositionCount = 0;
        int[] deletePositions = new int[positionCount];
        int deletePositionCount = 0;
        int[] updatePositions = new int[positionCount];
        int updatePositionCount = 0;

        for (int position = 0; position < positionCount; position++) {
            int operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                }
                case DELETE_OPERATION_NUMBER -> {
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                }
                case UPDATE_OPERATION_NUMBER -> {
                    updatePositions[updatePositionCount] = position;
                    updatePositionCount++;
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }

        if (insertPositionCount > 0) {
            insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertPositionCount));
        }

        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columnCount + 1));
        if (deletePositionCount > 0) {
            Block[] deleteBlocks = new Block[rowIdFields.size()];
            for (int field = 0; field < rowIdFields.size(); field++) {
                deleteBlocks[field] = rowIdFields.get(field).getPositions(deletePositions, 0, deletePositionCount);
            }
            deleteSink.appendPage(new Page(deletePositionCount, deleteBlocks));
        }

        if (updatePositionCount > 0) {
            Page updatePage = dataPage.getPositions(updatePositions, 0, updatePositionCount);
            if (hasRowKey) {
                updatePage = updatePage.appendColumn(rowIdFields.get(0).getPositions(updatePositions, 0, updatePositionCount));
            }

            updateSink.appendPage(updatePage);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish();
        deleteSink.finish();
        updateSink.finish();
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        insertSink.abort();
        deleteSink.abort();
        updateSink.abort();
    }
}
