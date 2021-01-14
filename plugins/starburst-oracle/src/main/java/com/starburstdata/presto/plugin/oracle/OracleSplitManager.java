/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.math.IntMath;
import com.starburstdata.presto.license.LicenseManager;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.starburstdata.presto.license.StarburstPrestoFeature.ORACLE_EXTENSIONS;
import static com.starburstdata.presto.plugin.jdbc.dynamicfiltering.DynamicFilteringSplitManager.isColumnEligibleForDynamicFilter;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.NO_PARALLELISM;
import static com.starburstdata.presto.plugin.oracle.OracleParallelismType.PARTITIONS;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.getMaxSplitsPerScan;
import static com.starburstdata.presto.plugin.oracle.StarburstOracleSessionProperties.getParallelismType;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;

public class OracleSplitManager
        implements ConnectorSplitManager
{
    private final ConnectionFactory connectionFactory;

    @Inject
    public OracleSplitManager(
            ConnectionFactory connectionFactory,
            StarburstOracleConfig starburstOracleConfig,
            LicenseManager licenseManager)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        if (starburstOracleConfig.getParallelismType() != OracleParallelismType.NO_PARALLELISM) {
            licenseManager.checkFeature(ORACLE_EXTENSIONS);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        return new FixedSplitSource(listSplits(
                session,
                (JdbcTableHandle) table,
                getParallelismType(session),
                getMaxSplitsPerScan(session),
                dynamicFilter.getCurrentPredicate()
                        .filter((columnHandle, domain) -> isColumnEligibleForDynamicFilter((JdbcTableHandle) table, (JdbcColumnHandle) columnHandle))));
    }

    private List<OracleSplit> listSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            OracleParallelismType parallelismType,
            int maxSplits,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        if (parallelismType == NO_PARALLELISM || tableHandle.getGroupingSets().isPresent()) {
            return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty(), dynamicFilter));
        }

        if (parallelismType == PARTITIONS) {
            List<String> partitions = listPartitionsForTable(session, tableHandle);

            if (partitions.isEmpty()) {
                // Table is not partitioned
                return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty(), dynamicFilter));
            }

            List<String> duplicatedPartitions = getDuplicates(partitions);
            verify(duplicatedPartitions.isEmpty(), "Partition names are not unique for table %s: %s", tableHandle, duplicatedPartitions);

            // Partition partitions into batches to limit total number of splits
            return Lists.partition(partitions, IntMath.divide(partitions.size(), maxSplits, CEILING)).stream()
                    .map(batch -> new OracleSplit(Optional.of(batch), Optional.empty(), dynamicFilter))
                    .collect(toImmutableList());
        }

        throw new IllegalArgumentException(format("Parallelism type %s is not supported", parallelismType));
    }

    private List<String> getDuplicates(List<String> values)
    {
        return values.stream()
                .collect(toImmutableMultiset()).entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableList());
    }

    private List<String> listPartitionsForTable(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(session))) {
            return handle.createQuery("SELECT partition_name FROM all_tab_partitions WHERE table_name = :name AND table_owner = :owner")
                    .bind("name", tableHandle.getTableName())
                    .bind("owner", tableHandle.getSchemaName())
                    .mapTo(String.class)
                    .list();
        }
        catch (JdbiException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
