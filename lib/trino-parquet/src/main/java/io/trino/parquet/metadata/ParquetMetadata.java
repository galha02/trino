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
package io.trino.parquet.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.spi.TrinoException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.ParquetMetadataConverter.convertEncodingStats;
import static io.trino.parquet.ParquetMetadataConverter.getEncoding;
import static io.trino.parquet.ParquetMetadataConverter.getLogicalTypeAnnotation;
import static io.trino.parquet.ParquetMetadataConverter.getPrimitive;
import static io.trino.parquet.ParquetMetadataConverter.toColumnIndexReference;
import static io.trino.parquet.ParquetMetadataConverter.toOffsetIndexReference;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.reader.MetadataReader.readStats;
import static io.trino.spi.StandardErrorCode.CORRUPT_PAGE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ParquetMetadata
{
    private static final Logger log = Logger.get(ParquetMetadata.class);

    private final FileMetaData fileMetaData;
    private final ParquetDataSourceId dataSourceId;
    private final Optional<Long> offset;
    private final Optional<Long> length;
    private final FileMetadata parquetMetadata;

    public ParquetMetadata(FileMetaData fileMetaData, ParquetDataSourceId dataSourceId, Optional<Long> offset, Optional<Long> length)
            throws ParquetCorruptionException
    {
        this.fileMetaData = requireNonNull(fileMetaData, "fileMetaData is null");
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.offset = requireNonNull(offset, "offset is null");
        this.length = requireNonNull(length, "length is null");
        checkArgument(offset.isEmpty() && length.isEmpty() || (offset.isPresent() && length.isPresent()),
                "Both offset and length must be present or absent");
        this.parquetMetadata = new FileMetadata(readMessageType(), keyValueMetaData(fileMetaData), fileMetaData.getCreated_by());
    }

    private static Map<String, String> keyValueMetaData(FileMetaData fileMetaData)
    {
        if (fileMetaData.getKey_value_metadata() == null) {
            return ImmutableMap.of();
        }
        return fileMetaData.getKey_value_metadata().stream().collect(toMap(KeyValue::getKey, KeyValue::getValue));
    }

    private MessageType readMessageType()
            throws ParquetCorruptionException
    {
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    public FileMetadata getFileMetaData()
    {
        return parquetMetadata;
    }

    public List<BlockMetadata> getBlocks(Collection<ColumnDescriptor> columnDescriptors)
    {
        Set<ColumnPath> paths = columnDescriptors.stream()
                .map(ColumnDescriptor::getPath)
                .map(ColumnPath::get)
                .collect(toImmutableSet());

        try {
            return buildBlocks(paths);
        }
        catch (ParquetCorruptionException e) {
            throw new TrinoException(CORRUPT_PAGE, e);
        }
    }

    public List<BlockMetadata> getBlocks()
    {
        return getBlocks(ImmutableSet.of());
    }

    private List<BlockMetadata> buildBlocks(Set<ColumnPath> paths)
            throws ParquetCorruptionException
    {
        List<SchemaElement> schema = fileMetaData.getSchema();
        validateParquet(!schema.isEmpty(), dataSourceId, "Schema is empty");

        MessageType messageType = readParquetSchema(schema);
        ImmutableList.Builder<BlockMetadata> builder = ImmutableList.builder();
        List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        if (rowGroups != null) {
            for (int i = 0; i < rowGroups.size(); i++) {
                RowGroup rowGroup = rowGroups.get(i);
                List<ColumnChunk> columns = rowGroup.getColumns();
                validateParquet(!columns.isEmpty(), dataSourceId, "No columns in row group: %s", rowGroup);
                String filePath = columns.getFirst().getFile_path();

                if (offset.isPresent() && length.isPresent() && rowGroup.isSetFile_offset()) {
                    if (rowGroup.file_offset >= offset.get() + length.get()) {
                        break;
                    }
                    if (i < rowGroups.size() - 1 && offset.get() >= rowGroups.get(i + 1).file_offset) {
                        continue;
                    }
                }

                ImmutableList.Builder<ColumnChunkMetadata> columnMetadataBuilder = ImmutableList.builderWithExpectedSize(columns.size());
                for (ColumnChunk columnChunk : columns) {
                    validateParquet(
                            (filePath == null && columnChunk.getFile_path() == null)
                                    || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                            dataSourceId,
                            "all column chunks of the same row group must be in the same file");
                    ColumnMetaData metaData = columnChunk.meta_data;
                    String[] path = metaData.path_in_schema.stream()
                            .map(value -> value.toLowerCase(Locale.ENGLISH))
                            .toArray(String[]::new);
                    ColumnPath columnPath = ColumnPath.get(path);
                    if (!paths.isEmpty() && !paths.contains(columnPath)) {
                        continue;
                    }
                    PrimitiveType primitiveType = messageType.getType(columnPath.toArray()).asPrimitiveType();
                    ColumnChunkMetadata column = ColumnChunkMetadata.get(
                            columnPath,
                            primitiveType,
                            CompressionCodecName.fromParquet(metaData.codec),
                            convertEncodingStats(metaData.encoding_stats),
                            readEncodings(metaData.encodings),
                            readStats(Optional.ofNullable(fileMetaData.getCreated_by()), Optional.ofNullable(metaData.statistics), primitiveType),
                            metaData.data_page_offset,
                            metaData.dictionary_page_offset,
                            metaData.num_values,
                            metaData.total_compressed_size,
                            metaData.total_uncompressed_size);
                    column.setColumnIndexReference(toColumnIndexReference(columnChunk));
                    column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));
                    column.setBloomFilterOffset(metaData.bloom_filter_offset);
                    columnMetadataBuilder.add(column);
                }
                builder.add(new BlockMetadata(rowGroup.getNum_rows(), columnMetadataBuilder.build()));
            }
        }
        return builder.build();
    }

    private static Set<org.apache.parquet.column.Encoding> readEncodings(List<Encoding> encodings)
    {
        Set<org.apache.parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (Encoding encoding : encodings) {
            columnEncodings.add(getEncoding(encoding));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static MessageType readParquetSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Type.Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getPrimitive(element.type), Type.Repetition.valueOf(element.repetition_type.name()));
                if (element.isSetType_length()) {
                    primitiveBuilder.length(element.type_length);
                }
                if (element.isSetPrecision()) {
                    primitiveBuilder.precision(element.precision);
                }
                if (element.isSetScale()) {
                    primitiveBuilder.scale(element.scale);
                }
                typeBuilder = primitiveBuilder;
            }

            // Reading of element.logicalType and element.converted_type corresponds to parquet-mr's code at
            // https://github.com/apache/parquet-mr/blob/apache-parquet-1.12.0/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L1568-L1582
            LogicalTypeAnnotation annotationFromLogicalType = null;
            if (element.isSetLogicalType()) {
                annotationFromLogicalType = getLogicalTypeAnnotation(element.logicalType);
                typeBuilder.as(annotationFromLogicalType);
            }
            if (element.isSetConverted_type()) {
                LogicalTypeAnnotation annotationFromConvertedType = getLogicalTypeAnnotation(element.converted_type, element);
                if (annotationFromLogicalType != null) {
                    // Both element.logicalType and element.converted_type set
                    if (annotationFromLogicalType.toOriginalType() == annotationFromConvertedType.toOriginalType()) {
                        // element.converted_type matches element.logicalType, even though annotationFromLogicalType may differ from annotationFromConvertedType
                        // Following parquet-mr behavior, we favor LogicalTypeAnnotation derived from element.logicalType, as potentially containing more information.
                    }
                    else {
                        // Following parquet-mr behavior, issue warning and let converted_type take precedence.
                        log.warn("Converted type and logical type metadata map to different OriginalType (convertedType: %s, logical type: %s). Using value in converted type.",
                                element.converted_type, element.logicalType);
                        // parquet-mr reads only OriginalType from converted_type. We retain full LogicalTypeAnnotation
                        // 1. for compatibility, as previous Trino reader code would read LogicalTypeAnnotation from element.converted_type and some additional fields.
                        // 2. so that we override LogicalTypeAnnotation annotation read from element.logicalType in case of mismatch detected.
                        typeBuilder.as(annotationFromConvertedType);
                    }
                }
                else {
                    // parquet-mr reads only OriginalType from converted_type. We retain full LogicalTypeAnnotation for compatibility, as previous
                    // Trino reader code would read LogicalTypeAnnotation from element.converted_type and some additional fields.
                    typeBuilder.as(annotationFromConvertedType);
                }
            }

            if (element.isSetField_id()) {
                typeBuilder.id(element.field_id);
            }
            typeBuilder.named(element.name.toLowerCase(Locale.ENGLISH));
        }
    }

    @Override
    public String toString()
    {
        return "ParquetMetaData{" + fileMetaData + "}";
    }
}
