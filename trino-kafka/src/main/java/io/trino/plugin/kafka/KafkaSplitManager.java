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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kafka.schema.ContentSchemaReader;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final ContentSchemaReader contentSchemaReader;
    private final int messagesPerSplit;

    @Inject
    public KafkaSplitManager(KafkaConsumerFactory consumerFactory, KafkaConfig kafkaConfig, KafkaFilterManager kafkaFilterManager, ContentSchemaReader contentSchemaReader)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerManager is null");
        this.messagesPerSplit = requireNonNull(kafkaConfig, "kafkaConfig is null").getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.contentSchemaReader = requireNonNull(contentSchemaReader, "contentSchemaReader is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create()) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());

            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            KafkaFilteringResult kafkaFilteringResult = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets);
            partitionInfos = kafkaFilteringResult.getPartitionInfos();
            partitionBeginOffsets = kafkaFilteringResult.getPartitionBeginOffsets();
            partitionEndOffsets = kafkaFilteringResult.getPartitionEndOffsets();

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaReader.readKeyContentSchema(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaReader.readValueContentSchema(kafkaTableHandle);

            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                new Range(partitionBeginOffsets.get(topicPartition), partitionEndOffsets.get(topicPartition))
                        .partition(messagesPerSplit).stream()
                        .map(range -> new KafkaSplit(
                                kafkaTableHandle.getTopicName(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                keyDataSchemaContents,
                                messageDataSchemaContents,
                                partitionInfo.partition(),
                                range,
                                leader))
                        .forEach(splits::add);
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
