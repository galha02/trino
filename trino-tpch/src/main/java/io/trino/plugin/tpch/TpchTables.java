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
package io.trino.plugin.tpch;

import com.google.common.collect.AbstractIterator;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.tpch.TpchTable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.tpch.TpchRecordSet.createTpchRecordSet;

public final class TpchTables
{
    private TpchTables()
    {
    }

    public static List<Type> getTableColumns(String tableName)
    {
        TpchTable<?> table = TpchTable.getTable(tableName);
        return table.getColumns().stream()
                .map(TpchMetadata::getPrestoType)
                .collect(toImmutableList());
    }

    public static Iterator<Page> getTablePages(
            String tableName,
            double scaleFactor)
    {
        TpchTable table = TpchTable.getTable(tableName);
        ConnectorPageSource pageSource = new RecordPageSource(
                createTpchRecordSet(table, table.getColumns(), scaleFactor, 1, 1, TupleDomain.all()));
        return new AbstractIterator<>()
        {
            @Override
            protected Page computeNext()
            {
                if (pageSource.isFinished()) {
                    return endOfData();
                }

                Page page = pageSource.getNextPage();
                if (page == null) {
                    return computeNext();
                }

                return page.getLoadedPage();
            }
        };
    }
}
