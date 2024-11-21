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
package io.trino.plugin.lance;

import com.google.inject.Inject;
import com.lancedb.lance.DatasetFragment;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private final LanceReader lanceReader;
    private final LanceConfig lanceConfig;

    @Inject
    public LanceSplitManager(LanceReader lanceReader, LanceConfig lanceConfig)
    {
        this.lanceReader = requireNonNull(lanceReader, "reader is null");
        this.lanceConfig = requireNonNull(lanceConfig, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorTableHandle tableHandle, DynamicFilter dynamicFilter, Constraint constraint)
    {
        if (lanceConfig.getConnectorType() == LanceConfig.Type.FRAGMENT) {
            List<Integer> fragmentIds = lanceReader.getFragments((LanceTableHandle) tableHandle)
                    .stream().map(DatasetFragment::getId).toList();
            return new FixedSplitSource(fragmentIds.stream().map(id -> new LanceSplit(Collections.singletonList(id))).toList());
        }
        else {
            // use empty list to indicate dataset based page source split
            return new FixedSplitSource(new LanceSplit(Collections.emptyList()));
        }
    }
}
