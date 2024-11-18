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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class KafkaSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(KafkaSplit.class);

    private final String topicName;
    private final String keyDataFormat;
    private final String messageDataFormat;
    private final Optional<String> keyDataSchemaContents;
    private final Optional<String> messageDataSchemaContents;
    private final int partitionId;
    private final Range messagesRange;
    private final HostAddress leader;
    private final OptionalLong limit;

    @JsonCreator
    public KafkaSplit(
            @JsonProperty("topicName") String topicName,
            @JsonProperty("keyDataFormat") String keyDataFormat,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("keyDataSchemaContents") Optional<String> keyDataSchemaContents,
            @JsonProperty("messageDataSchemaContents") Optional<String> messageDataSchemaContents,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("messagesRange") Range messagesRange,
            @JsonProperty("leader") HostAddress leader,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.keyDataSchemaContents = requireNonNull(keyDataSchemaContents, "keyDataSchemaContents is null");
        this.messageDataSchemaContents = requireNonNull(messageDataSchemaContents, "messageDataSchemaContents is null");
        this.partitionId = partitionId;
        this.messagesRange = requireNonNull(messagesRange, "messagesRange is null");
        this.leader = requireNonNull(leader, "leader is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    /**
     * Returns a KafkaSplit object for the given messagesRange.
     *
     * @param messagesRange The range of messages for the split.
     * @return A KafkaSplit object initialized with the appropriate properties.
     */
    public KafkaSplit getSplitByRange(Range messagesRange)
    {
        return new KafkaSplit(
                getTopicName(),
                getKeyDataFormat(),
                getMessageDataFormat(),
                getKeyDataSchemaContents(),
                getMessageDataSchemaContents(),
                getPartitionId(),
                messagesRange,
                getLeader(),
                getLimit());
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getKeyDataFormat()
    {
        return keyDataFormat;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public Optional<String> getKeyDataSchemaContents()
    {
        return keyDataSchemaContents;
    }

    @JsonProperty
    public Optional<String> getMessageDataSchemaContents()
    {
        return messageDataSchemaContents;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public Range getMessagesRange()
    {
        return messagesRange;
    }

    @JsonProperty
    public HostAddress getLeader()
    {
        return leader;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(leader);
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of("topicName", topicName, "partitionId", String.valueOf(partitionId), "leader", leader.toString());
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(topicName)
                + estimatedSizeOf(keyDataFormat)
                + estimatedSizeOf(messageDataFormat)
                + sizeOf(keyDataSchemaContents, SizeOf::estimatedSizeOf)
                + sizeOf(messageDataSchemaContents, SizeOf::estimatedSizeOf)
                + messagesRange.retainedSizeInBytes()
                + leader.getRetainedSizeInBytes();
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("topicName", topicName)
                .add("keyDataFormat", keyDataFormat)
                .add("messageDataFormat", messageDataFormat)
                .add("keyDataSchemaContents", keyDataSchemaContents)
                .add("messageDataSchemaContents", messageDataSchemaContents)
                .add("partitionId", partitionId)
                .add("messagesRange", messagesRange)
                .add("leader", leader)
                .toString();
    }
}
