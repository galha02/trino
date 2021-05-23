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

package io.trino.plugin.base.classloader;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ConnectorRecordSetProvider delegate;
    private final ClassLoader classLoader;

    @Inject
    public ClassLoaderSafeConnectorRecordSetProvider(@ForClassLoaderSafe ConnectorRecordSetProvider delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        return getRecordSet(transaction, session, split, table, columns);
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new ClassLoaderSafeRecordSet(delegate.getRecordSet(transaction, session, split, table, columns), classLoader);
        }
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new ClassLoaderSafeRecordSet(delegate.getRecordSet(transactionHandle, session, split, columns), classLoader);
        }
    }
}
