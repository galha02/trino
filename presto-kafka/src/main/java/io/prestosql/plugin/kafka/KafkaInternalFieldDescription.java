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
package io.prestosql.plugin.kafka;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
 * show up in the tables (the columns are hidden by default, so they must be explicitly selected) but unless the field is hooked in using the
 * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
 * will be null. Most values are assigned in the {@link io.prestosql.plugin.kafka.KafkaRecordSet}.
 */
public enum KafkaInternalFieldDescription
{
    /**
     * <tt>_partition_id</tt> - Kafka partition id.
     */
    PARTITION_ID_FIELD("_partition_id", BigintType.BIGINT, "Partition Id"),

    /**
     * <tt>_timestamp</tt> - Kafka message timestamp.
     */
    TIMESTAMP_FIELD("_timestamp", BigintType.BIGINT, "Message creation timestamp"),

    /**
     * <tt>_partition_offset</tt> - The current offset of the message in the partition.
     */
    PARTITION_OFFSET_FIELD("_partition_offset", BigintType.BIGINT, "Offset for the message within the partition"),

    /**
     * <tt>_segment_start</tt> - Kafka start offset for the segment which contains the current message. This is per-partition.
     */
    SEGMENT_START_FIELD("_segment_start", BigintType.BIGINT, "Segment start offset"),

    /**
     * <tt>_segment_end</tt> - Kafka end offset for the segment which contains the current message. This is per-partition. The end offset is the first offset that is *not* in the segment.
     */
    SEGMENT_END_FIELD("_segment_end", BigintType.BIGINT, "Segment end offset"),

    /**
     * <tt>_segment_count</tt> - Running count of messages in a segment.
     */
    SEGMENT_COUNT_FIELD("_segment_count", BigintType.BIGINT, "Running message count per segment"),

    /**
     * <tt>_message_corrupt</tt> - True if the row converter could not read the a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    MESSAGE_CORRUPT_FIELD("_message_corrupt", BooleanType.BOOLEAN, "Message data is corrupt"),

    /**
     * <tt>_message</tt> - Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO: make charset configurable.
     */
    MESSAGE_FIELD("_message", createUnboundedVarcharType(), "Message text"),

    /**
     * <tt>_message_length</tt> - length in bytes of the message.
     */
    MESSAGE_LENGTH_FIELD("_message_length", BigintType.BIGINT, "Total number of message bytes"),

    /**
     * <tt>_key_corrupt</tt> - True if the row converter could not read the a key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    KEY_CORRUPT_FIELD("_key_corrupt", BooleanType.BOOLEAN, "Key data is corrupt"),

    /**
     * <tt>_key</tt> - Represents the key as a text column. Format is UTF-8 which may be wrong for topics. TODO: make charset configurable.
     */
    KEY_FIELD("_key", createUnboundedVarcharType(), "Key text"),

    /**
     * <tt>_key_length</tt> - length in bytes of the key.
     */
    KEY_LENGTH_FIELD("_key_length", BigintType.BIGINT, "Total number of key bytes");

    private static final Map<String, KafkaInternalFieldDescription> BY_COLUMN_NAME =
            stream(KafkaInternalFieldDescription.values())
                    .collect(toImmutableMap(KafkaInternalFieldDescription::getColumnName, identity()));

    public static KafkaInternalFieldDescription forColumnName(String columnName)
    {
        KafkaInternalFieldDescription description = BY_COLUMN_NAME.get(columnName);
        checkArgument(description != null, "Unknown internal column name %s", columnName);
        return description;
    }

    private final String columnName;
    private final Type type;
    private final String comment;

    KafkaInternalFieldDescription(
            String columnName,
            Type type,
            String comment)
    {
        checkArgument(!isNullOrEmpty(columnName), "name is null or is empty");
        this.columnName = columnName;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    private Type getType()
    {
        return type;
    }

    KafkaColumnHandle getColumnHandle(int index, boolean hidden)
    {
        return new KafkaColumnHandle(
                index,
                getColumnName(),
                getType(),
                null,
                null,
                null,
                false,
                hidden,
                true);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(type)
                .setComment(Optional.ofNullable(comment))
                .setHidden(hidden)
                .build();
    }
}
