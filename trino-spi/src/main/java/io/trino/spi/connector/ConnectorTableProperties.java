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
package io.trino.spi.connector;

import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ConnectorTableProperties
{
    private final TupleDomain<ColumnHandle> predicate;
    private final Optional<ConnectorTablePartitioning> tablePartitioning;
    private final Optional<Set<ColumnHandle>> streamPartitioningColumns;
    private final Optional<DiscretePredicates> discretePredicates;
    private final List<LocalProperty<ColumnHandle>> localProperties;

    public ConnectorTableProperties()
    {
        this(TupleDomain.all(), Optional.empty(), Optional.empty(), Optional.empty(), emptyList());
    }

    @Deprecated
    public ConnectorTableProperties(ConnectorTableLayout layout)
    {
        this(layout.getPredicate(),
                layout.getTablePartitioning(),
                layout.getStreamPartitioningColumns(),
                layout.getDiscretePredicates(),
                layout.getLocalProperties());
    }

    public ConnectorTableProperties(
            TupleDomain<ColumnHandle> predicate,
            Optional<ConnectorTablePartitioning> tablePartitioning,
            Optional<Set<ColumnHandle>> streamPartitioningColumns,
            Optional<DiscretePredicates> discretePredicates,
            List<LocalProperty<ColumnHandle>> localProperties)
    {
        requireNonNull(streamPartitioningColumns, "partitioningColumns is null");
        requireNonNull(tablePartitioning, "tablePartitioning is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(discretePredicates, "discretePredicates is null");
        requireNonNull(localProperties, "localProperties is null");

        this.tablePartitioning = tablePartitioning;
        this.streamPartitioningColumns = streamPartitioningColumns;
        this.predicate = predicate;
        this.discretePredicates = discretePredicates;
        this.localProperties = localProperties;
    }

    /**
     * A TupleDomain that represents a predicate that every row in this table satisfies.
     * <p>
     * This guarantee can have different origins.
     * For example, it may be successful predicate push down, or inherent guarantee provided by the underlying data.
     */
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }

    /**
     * The partitioning of the table across the worker nodes.
     * <p>
     * If the table is node partitioned, the connector guarantees that each combination of values for
     * the distributed columns will be contained within a single worker.
     */
    public Optional<ConnectorTablePartitioning> getTablePartitioning()
    {
        return tablePartitioning;
    }

    /**
     * The partitioning for the table streams.
     * If empty, the table layout is partitioned arbitrarily.
     * Otherwise, table steams are partitioned on the given set of columns (or unpartitioned, if the set is empty)
     * <p>
     * If the table is partitioned, the connector guarantees that each combination of values for
     * the partition columns will be contained within a single split (i.e., partitions cannot
     * straddle multiple splits)
     */
    public Optional<Set<ColumnHandle>> getStreamPartitioningColumns()
    {
        return streamPartitioningColumns;
    }

    /**
     * A collection of discrete predicates describing the data in this layout. The union of
     * these predicates is expected to be equivalent to the overall predicate returned
     * by {@link #getPredicate()}. They may be used by the engine for further optimizations.
     */
    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return discretePredicates;
    }

    /**
     * Properties describing the layout of the data (grouping/sorting) within each partition
     */
    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return localProperties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicate, discretePredicates, streamPartitioningColumns, tablePartitioning, localProperties);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConnectorTableProperties other = (ConnectorTableProperties) obj;
        return Objects.equals(this.predicate, other.predicate)
                && Objects.equals(this.discretePredicates, other.discretePredicates)
                && Objects.equals(this.streamPartitioningColumns, other.streamPartitioningColumns)
                && Objects.equals(this.tablePartitioning, other.tablePartitioning)
                && Objects.equals(this.localProperties, other.localProperties);
    }
}
