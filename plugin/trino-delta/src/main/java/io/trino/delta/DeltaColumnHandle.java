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
package io.trino.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.TypeSignature;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DeltaColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String name;
    private final TypeSignature dataType;
    private final ColumnType columnType;

    public enum ColumnType
    {
        REGULAR,
        PARTITION,
        SUBFIELD,
    }

    @JsonCreator
    public DeltaColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String name,
            @JsonProperty("dataType") TypeSignature dataType,
            @JsonProperty("columnType") ColumnType columnType)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeSignature getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("dataType", dataType)
                .add("columnType", columnType)
                .toString();
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

        DeltaColumnHandle that = (DeltaColumnHandle) o;
        return connectorId.equals(that.connectorId) &&
                name.equals(that.name) &&
                dataType.equals(that.dataType) &&
                columnType == that.columnType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, name, dataType, columnType);
    }
}
