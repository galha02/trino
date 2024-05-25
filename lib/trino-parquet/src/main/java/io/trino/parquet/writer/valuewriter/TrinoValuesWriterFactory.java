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
package io.trino.parquet.writer.valuewriter;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;

import java.util.Optional;

import static io.trino.parquet.writer.valuewriter.BloomFilterValuesWriter.createBloomFilterValuesWriter;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;

/**
 * Based on org.apache.parquet.column.values.factory.DefaultV1ValuesWriterFactory
 */
public class TrinoValuesWriterFactory
{
    private final ParquetProperties parquetProperties;

    public TrinoValuesWriterFactory(ParquetProperties properties)
    {
        this.parquetProperties = properties;
    }

    public ValuesWriter newValuesWriter(ColumnDescriptor descriptor, Optional<BloomFilter> bloomFilter)
    {
        return switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN -> new BooleanPlainValuesWriter(); // no dictionary encoding for boolean
            case FIXED_LEN_BYTE_ARRAY -> getFixedLenByteArrayValuesWriter(descriptor, bloomFilter);
            case BINARY -> getBinaryValuesWriter(descriptor, bloomFilter);
            case INT32 -> getInt32ValuesWriter(descriptor, bloomFilter);
            case INT64 -> getInt64ValuesWriter(descriptor, bloomFilter);
            case INT96 -> getInt96ValuesWriter(descriptor, bloomFilter);
            case DOUBLE -> getDoubleValuesWriter(descriptor, bloomFilter);
            case FLOAT -> getFloatValuesWriter(descriptor, bloomFilter);
        };
    }

    private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        // dictionary encoding was not enabled in PARQUET 1.0
        return createBloomFilterValuesWriter(new FixedLenByteArrayPlainValuesWriter(path.getPrimitiveType().getTypeLength(), parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
    }

    private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new FixedLenByteArrayPlainValuesWriter(12, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    private ValuesWriter getFloatValuesWriter(ColumnDescriptor path, Optional<BloomFilter> bloomFilter)
    {
        ValuesWriter fallbackWriter = createBloomFilterValuesWriter(new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator()), bloomFilter);
        return dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }

    @SuppressWarnings("deprecation")
    private static Encoding getEncodingForDataPage()
    {
        return PLAIN_DICTIONARY;
    }

    @SuppressWarnings("deprecation")
    private static Encoding getEncodingForDictionaryPage()
    {
        return PLAIN_DICTIONARY;
    }

    private static DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, ParquetProperties properties, Encoding dictPageEncoding, Encoding dataPageEncoding)
    {
        return switch (path.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN -> throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
            case BINARY ->
                    new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case INT32 ->
                    new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case INT64 ->
                    new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case INT96 ->
                    new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), 12, dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case DOUBLE ->
                    new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case FLOAT ->
                    new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
            case FIXED_LEN_BYTE_ARRAY ->
                    new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(properties.getDictionaryPageSizeThreshold(), path.getPrimitiveType().getTypeLength(), dataPageEncoding, dictPageEncoding, properties.getAllocator());
        };
    }

    private static ValuesWriter dictWriterWithFallBack(ColumnDescriptor path, ParquetProperties parquetProperties, Encoding dictPageEncoding, Encoding dataPageEncoding, ValuesWriter writerToFallBackTo)
    {
        return new DictionaryFallbackValuesWriter(dictionaryWriter(path, parquetProperties, dictPageEncoding, dataPageEncoding), writerToFallBackTo);
    }
}
