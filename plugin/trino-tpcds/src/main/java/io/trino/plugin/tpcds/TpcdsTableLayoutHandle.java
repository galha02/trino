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
package io.trino.plugin.tpcds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TpcdsTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TpcdsTableHandle table;

    @JsonCreator
    public TpcdsTableLayoutHandle(@JsonProperty("table") TpcdsTableHandle table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public TpcdsTableHandle getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return table.getTableName();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TpcdsTableLayoutHandle that = (TpcdsTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return table.hashCode();
    }
}
