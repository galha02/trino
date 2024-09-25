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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.IntegerType;
import jakarta.inject.Inject;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.commons.lang.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_ADMIN_ERROR;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_SPLIT_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries;

/**
 * The class helping to manage Trino Pulsar splits.
 */
public class PulsarSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    //private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarSplitManager.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public PulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorTableHandle table,
        DynamicFilter dynamicFilter,
        Constraint constraint) {

        int numSplits = this.pulsarConnectorConfig.getTargetNumSplits();

        //PulsarTableLayoutHandle layoutHandle = (PulsarTableLayoutHandle) layout;
        PulsarTableHandle tableHandle = (PulsarTableHandle)table;
        /*???????*/TupleDomain<ColumnHandle> tupleDomain = layoutHandle.getTupleDomain();

        String namespace = restoreNamespaceDelimiterIfNeeded(tableHandle.getSchemaName(), pulsarConnectorConfig);
        TopicName topicName = TopicName.get("persistent", NamespaceName.get(namespace), tableHandle.getTopicName());

        SchemaInfo schemaInfo;

        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)){
            schemaInfo = pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", namespace, tableHandle.getTopicName()));
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new TrinoException(QUERY_REJECTED,
                        String.format("Failed to get pulsar topic schema for topic %s/%s: Unauthorized",
                                namespace, tableHandle.getTopicName()));
            } else if (e.getStatusCode() == 404) {
                schemaInfo = PulsarSqlSchemaInfoProvider.defaultSchema();
            } else {
                throw new RuntimeException("Failed to get pulsar topic schema for topic "
                        + String.format("%s/%s", namespace, tableHandle.getTopicName())
                        + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
            }
        }

        Collection<PulsarSplit> splits;
        try {
            OffloadPoliciesImpl offloadPolicies = (OffloadPoliciesImpl) this.pulsarAdmin.namespaces()
                                                .getOffloadPolicies(topicName.getNamespace());
            if (offloadPolicies != null) {
                offloadPolicies.setOffloadersDirectory(pulsarConnectorConfig.getOffloadersDirectory());
                offloadPolicies.setManagedLedgerOffloadMaxThreads(
                        pulsarConnectorConfig.getManagedLedgerOffloadMaxThreads());
            }
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                splits = getSplitsNonPartitionedTopic(
                        numSplits, topicName, tableHandle, schemaInfo, tupleDomain, offloadPolicies);
                log.debug("Splits for non-partitioned topic %s: %s", topicName, splits);
            } else {
                splits = getSplitsPartitionedTopic(
                        numSplits, topicName, tableHandle, schemaInfo, tupleDomain, offloadPolicies);
                log.debug("Splits for partitioned topic %s: %s", topicName, splits);
            }
        } catch (Exception e) {
            log.error(e, "Failed to get splits");
            throw new RuntimeException(e);
        }
        return new FixedSplitSource(splits);
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle
            tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
              OffloadPoliciesImpl offloadPolicies) throws Exception {

        List<Integer> predicatedPartitions = getPredicatedPartitions(topicName, tupleDomain);
        if (log.isDebugEnabled()) {
            log.debug("Partition filter result %s", predicatedPartitions);
        }

        int actualNumSplits = Math.max(predicatedPartitions.size(), numSplits);

        int splitsPerPartition = actualNumSplits / predicatedPartitions.size();

        int splitRemainder = actualNumSplits % predicatedPartitions.size();

        PulsarConnectorCache pulsarConnectorCache = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig);
        ManagedLedgerFactory managedLedgerFactory = pulsarConnectorCache.getManagedLedgerFactory();
        ManagedLedgerConfig managedLedgerConfig = pulsarConnectorCache.getManagedLedgerConfig(
                topicName.getNamespaceObject(), offloadPolicies, pulsarConnectorConfig);

        List<PulsarSplit> splits = new LinkedList<>();
        for (int i = 0; i < predicatedPartitions.size(); i++) {
            int splitsForThisPartition = (splitRemainder > i) ? splitsPerPartition + 1 : splitsPerPartition;
            splits.addAll(
                getSplitsForTopic(
                    topicName.getPartition(predicatedPartitions.get(i)).getPersistenceNamingEncoding(),
                    managedLedgerFactory,
                    managedLedgerConfig,
                    splitsForThisPartition,
                    tableHandle,
                    schemaInfo,
                    topicName.getPartition(predicatedPartitions.get(i)).getLocalName(),
                    tupleDomain,
                    offloadPolicies));
        }
        return splits;
    }

    private List<Integer> getPredicatedPartitions(TopicName topicName, TupleDomain<ColumnHandle> tupleDomain) {
        int numPartitions;
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)){
            numPartitions = (pulsarAdmin.topics().getPartitionedTopicMetadata(topicName.toString())).partitions;
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new TrinoException(QUERY_REJECTED,
                    String.format("Failed to get metadata for partitioned topic %s: Unauthorized", topicName));
            }

            throw new RuntimeException("Failed to get metadata for partitioned topic "
                + topicName + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
        }
        List<Integer> predicatePartitions = new ArrayList<>();
        if (tupleDomain.getDomains().isPresent()) {
            Domain domain = tupleDomain.getDomains().get().get(PulsarInternalColumn.PARTITION
                .getColumnHandle(connectorId, false));
            if (domain != null) {
                domain.getValues().getValuesProcessor().consume(
                    ranges -> domain.getValues().getRanges().getOrderedRanges().forEach(range -> {
                        int low = 0;
                        int high = numPartitions;
                        if (range.getLowValue().isPresent()) {
                            Block block = Utils.nativeValueToBlock(range.getType(), range.getLowBoundedValue());
                            low = block.getInt(0, 0);
                        }
                        if (range.getHighValue().isPresent()) {
                            Block block = Utils.nativeValueToBlock(range.getType(), range.getHighBoundedValue());
                            high = block.getInt(0, 0);
                        }
                        for (int i = low; i <= high; i++) {
                            predicatePartitions.add(i);
                        }
                    }),
                    discreteValues -> {},
                    allOrNone -> {});
            } else {
                for (int i = 0; i < numPartitions; i++) {
                    predicatePartitions.add(i);
                }
            }
        } else {
            for (int i = 0; i < numPartitions; i++) {
                predicatePartitions.add(i);
            }
        }
        return predicatePartitions;
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName,
            PulsarTableHandle tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
             OffloadPoliciesImpl offloadPolicies) throws Exception {
        PulsarConnectorCache pulsarConnectorCache = PulsarConnectorCache.getConnectorCache(pulsarConnectorConfig);
        ManagedLedgerFactory managedLedgerFactory = pulsarConnectorCache.getManagedLedgerFactory();
        ManagedLedgerConfig managedLedgerConfig = pulsarConnectorCache.getManagedLedgerConfig(
                topicName.getNamespaceObject(), offloadPolicies, pulsarConnectorConfig);

        return getSplitsForTopic(
                topicName.getPersistenceNamingEncoding(),
                managedLedgerFactory,
                managedLedgerConfig,
                numSplits,
                tableHandle,
                schemaInfo,
                topicName.getLocalName(),
                tupleDomain,
                offloadPolicies);
    }

    @VisibleForTesting
    Collection<PulsarSplit> getSplitsForTopic(String topicNamePersistenceEncoding,
                                              ManagedLedgerFactory managedLedgerFactory,
                                              ManagedLedgerConfig managedLedgerConfig,
                                              int numSplits,
                                              PulsarTableHandle tableHandle,
                                              SchemaInfo schemaInfo, String tableName,
                                              TupleDomain<ColumnHandle> tupleDomain,
                                              OffloadPoliciesImpl offloadPolicies)
            throws ManagedLedgerException, InterruptedException, IOException {

        ReadOnlyCursor readOnlyCursor = null;
        try {
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    PositionImpl.EARLIEST, managedLedgerConfig);

            long numEntries = readOnlyCursor.getNumberOfEntries();
            if (numEntries <= 0) {
                return Collections.emptyList();
            }

            PredicatePushdownInfo predicatePushdownInfo = PredicatePushdownInfo.getPredicatePushdownInfo(
                    this.connectorId,
                    tupleDomain,
                    managedLedgerFactory,
                    managedLedgerConfig,
                    topicNamePersistenceEncoding,
                    numEntries);

            PositionImpl initialStartPosition;
            if (predicatePushdownInfo != null) {
                numEntries = predicatePushdownInfo.getNumOfEntries();
                initialStartPosition = predicatePushdownInfo.getStartPosition();
            } else {
                initialStartPosition = (PositionImpl) readOnlyCursor.getReadPosition();
            }


            readOnlyCursor.close();
            readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                    topicNamePersistenceEncoding,
                    initialStartPosition, new ManagedLedgerConfig());

            long remainder = numEntries % numSplits;

            long avgEntriesPerSplit = numEntries / numSplits;

            List<PulsarSplit> splits = new LinkedList<>();
            for (int i = 0; i < numSplits; i++) {
                long entriesForSplit = (remainder > i) ? avgEntriesPerSplit + 1 : avgEntriesPerSplit;
                PositionImpl startPosition = (PositionImpl) readOnlyCursor.getReadPosition();
                readOnlyCursor.skipEntries(Math.toIntExact(entriesForSplit));
                PositionImpl endPosition = (PositionImpl) readOnlyCursor.getReadPosition();

                PulsarSplit pulsarSplit = new PulsarSplit(i, this.connectorId,
                        restoreNamespaceDelimiterIfNeeded(tableHandle.getSchemaName(), pulsarConnectorConfig),
                        schemaInfo.getName(),
                        tableName,
                        entriesForSplit,
                        new String(schemaInfo.getSchema(),  "ISO8859-1"),
                        schemaInfo.getType(),
                        startPosition.getEntryId(),
                        endPosition.getEntryId(),
                        startPosition.getLedgerId(),
                        endPosition.getLedgerId(),
                        tupleDomain,
                        objectMapper.writeValueAsString(schemaInfo.getProperties()),
                        offloadPolicies);
                splits.add(pulsarSplit);
            }
            return splits;
        } finally {
            if (readOnlyCursor != null) {
                try {
                    readOnlyCursor.close();
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }

    @Data
    private static class PredicatePushdownInfo {
        private PositionImpl startPosition;
        private PositionImpl endPosition;
        private long numOfEntries;

        private PredicatePushdownInfo(PositionImpl startPosition, PositionImpl endPosition, long numOfEntries) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.numOfEntries = numOfEntries;
        }

        public static PredicatePushdownInfo getPredicatePushdownInfo(String connectorId,
                                                                     TupleDomain<ColumnHandle> tupleDomain,
                                                                     ManagedLedgerFactory managedLedgerFactory,
                                                                     ManagedLedgerConfig managedLedgerConfig,
                                                                     String topicNamePersistenceEncoding,
                                                                     long totalNumEntries) throws
                ManagedLedgerException, InterruptedException {

            ReadOnlyCursor readOnlyCursor = null;
            try {
                readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(
                        topicNamePersistenceEncoding,
                        PositionImpl.EARLIEST, managedLedgerConfig);

                if (tupleDomain.getDomains().isPresent()) {
                    Domain domain = tupleDomain.getDomains().get().get(PulsarInternalColumn.PUBLISH_TIME
                            .getColumnHandle(connectorId, false));
                    if (domain != null) {
                        // TODO support arbitrary number of ranges
                        // only worry about one range for now
                        if (domain.getValues().getRanges().getRangeCount() == 1) {

                            checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

                            Long upperBoundTs = null;
                            Long lowerBoundTs = null;

                            Range range = domain.getValues().getRanges().getOrderedRanges().get(0);

                            if (!range.isHighUnbounded()) {
                                Block block = Utils.nativeValueToBlock(range.getType(), range.getHighBoundedValue());
                                upperBoundTs = block.getLong(0, 0) / 1000;
                            }

                            if (!range.isLowUnbounded()) {
                                Block block = Utils.nativeValueToBlock(range.getType(), range.getLowBoundedValue());
                                lowerBoundTs = block.getLong(0, 0) / 1000;
                            }

                            PositionImpl overallStartPos;
                            if (lowerBoundTs == null) {
                                overallStartPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallStartPos = findPosition(readOnlyCursor, lowerBoundTs);
                                if (overallStartPos == null) {
                                    overallStartPos = (PositionImpl) readOnlyCursor.getReadPosition();
                                }
                            }

                            PositionImpl overallEndPos;
                            if (upperBoundTs == null) {
                                readOnlyCursor.skipEntries(Math.toIntExact(totalNumEntries));
                                overallEndPos = (PositionImpl) readOnlyCursor.getReadPosition();
                            } else {
                                overallEndPos = findPosition(readOnlyCursor, upperBoundTs);
                                if (overallEndPos == null) {
                                    overallEndPos = overallStartPos;
                                }
                            }

                            // Just use a close bound since presto can always filter out the extra entries even if
                            // the bound
                            // should be open or a mixture of open and closed
                            com.google.common.collect.Range<PositionImpl> posRange =
                                com.google.common.collect.Range.range(overallStartPos,
                                    com.google.common.collect.BoundType.CLOSED,
                                    overallEndPos, com.google.common.collect.BoundType.CLOSED);

                            long numOfEntries = readOnlyCursor.getNumberOfEntries(posRange) - 1;

                            PredicatePushdownInfo predicatePushdownInfo =
                                new PredicatePushdownInfo(overallStartPos, overallEndPos, numOfEntries);
                            log.debug("Predicate pushdown optimization calculated: %s", predicatePushdownInfo);
                            return predicatePushdownInfo;
                        }
                    }
                }
            } finally {
                if (readOnlyCursor != null) {
                    readOnlyCursor.close();
                }
            }
            return null;
        }
    }

    private static PositionImpl findPosition(ReadOnlyCursor readOnlyCursor, long timestamp) throws
            ManagedLedgerException,
            InterruptedException {
        return (PositionImpl) readOnlyCursor.findNewestMatching(
                SearchAllAvailableEntries,
                entry -> {
                    try {
                        long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                        return entryTimestamp <= timestamp;
                    } catch (Exception e) {
                        log.error(e, "Failed To deserialize message when finding position with error: %s", e);
                    } finally {
                        entry.release();
                    }
                    return false;
                });
    }
}
