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
package io.prestosql.plugin.google.sheets;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.TableNotFoundException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SheetsSplitManager
        implements ConnectorSplitManager
{
    private final SheetsClient sheetsClient;

    @Inject
    public SheetsSplitManager(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        SheetsTableHandle tableHandle = (SheetsTableHandle) connectorTableHandle;
        Optional<SheetsTable> table = sheetsClient.getTable(tableHandle.getTableName());

        // this can happen if table is removed during a query
        if (table.isEmpty()) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new SheetsSplit(tableHandle.getSchemaName(), tableHandle.getTableName(), table.get().getValues()));
        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }
}
