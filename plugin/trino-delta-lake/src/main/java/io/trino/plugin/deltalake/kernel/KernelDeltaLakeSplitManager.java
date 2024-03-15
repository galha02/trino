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
package io.trino.plugin.deltalake.kernel;

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class KernelDeltaLakeSplitManager
        implements ConnectorSplitManager
{
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        // TODO: dynamic filtering.
        return new DeltaSplitSource(session, (KernelDeltaLakeTableHandle) table);
    }

    private static class DeltaSplitSource
            implements ConnectorSplitSource
    {
        private final KernelDeltaLakeTableHandle deltaTable;

        // working state
        private String serializedScanState;
        private CloseableIterator<FilteredColumnarBatch> scanFileBatchIterator;
        private CloseableIterator<Row> scanFileIterator;

        DeltaSplitSource(ConnectorSession session, KernelDeltaLakeTableHandle deltaTableHandle)
        {
            this.deltaTable = deltaTableHandle;
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            List<ConnectorSplit> splits = new ArrayList<>();

            while (splits.size() < maxSize) {
                Optional<Row> scanFile = getNextScanFile();
                if (scanFile.isEmpty()) {
                    break;
                }
                splits.add(
                        new KernelDeltaLakeSplit(
                                deltaTable.getSchemaName(),
                                deltaTable.getTableName(),
                                deltaTable.getLocation(),
                                serializedScanState,
                                KernelRowSerDeUtils.serializeRowToJson(scanFile.get())));
            }

            return completedFuture(new ConnectorSplitBatch(splits, isFinished() /* noMoreSplits */));
        }

        @Override
        public void close()
        {
            Utils.closeCloseables(scanFileIterator, scanFileBatchIterator);
        }

        @Override
        public boolean isFinished()
        {
            return !scanFileIterator.hasNext() && !scanFileBatchIterator.hasNext();
        }

        private Optional<Row> getNextScanFile()
        {
            initScanFileBatchIteratorIfNotDone();
            if (scanFileIterator == null || !scanFileIterator.hasNext()) {
                Utils.closeCloseables(scanFileIterator);

                if (scanFileBatchIterator.hasNext()) {
                    scanFileIterator = scanFileBatchIterator.next().getRows();
                }
                else {
                    return Optional.empty();
                }
            }

            if (scanFileIterator.hasNext()) {
                return Optional.of(scanFileIterator.next());
            }
            else {
                return Optional.empty();
            }
        }

        private void initScanFileBatchIteratorIfNotDone()
        {
            if (scanFileBatchIterator == null) {
                Scan scan = KernelClient.createScan(deltaTable);
                Row scanState = scan.getScanState(KernelClient.getTableClient());
                scanFileBatchIterator = scan.getScanFiles(KernelClient.getTableClient());
                serializedScanState = KernelRowSerDeUtils.serializeRowToJson(scanState);
            }
        }
    }
}
