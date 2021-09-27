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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class DeltaTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final DeltaTable deltaTable;
    private final TupleDomain<DeltaColumnHandle> predicate;
    private final Optional<String> predicateString;

    @JsonCreator
    public DeltaTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("deltaTable") DeltaTable deltaTable,
            @JsonProperty("predicate") TupleDomain<DeltaColumnHandle> predicate,
            @JsonProperty("predicateString") Optional<String> predicateString)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.deltaTable = requireNonNull(deltaTable, "deltaTable is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.predicateString = requireNonNull(predicateString, "predicateString is null");
    }

    DeltaTableHandle withPredicate(TupleDomain<DeltaColumnHandle> predicate, String predicateString)
    {
        checkState(this.predicate.isAll(), "There is already a predicate.");
        return new DeltaTableHandle(
                connectorId,
                deltaTable,
                predicate,
                Optional.of(predicateString));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public DeltaTable getDeltaTable()
    {
        return deltaTable;
    }

    @JsonProperty
    public TupleDomain<DeltaColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<String> getPredicateString()
    {
        return predicateString;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, deltaTable, predicate);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DeltaTableHandle other = (DeltaTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.deltaTable, other.deltaTable) &&
                Objects.equals(this.predicate, other.predicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("table", toSchemaTableName())
                .add("predicate", predicateString)
                .toString();
    }
}
