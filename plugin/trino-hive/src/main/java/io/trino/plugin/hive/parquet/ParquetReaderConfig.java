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
package io.trino.plugin.hive.parquet;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.trino.parquet.ParquetReaderOptions;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

@DefunctConfig({
        "hive.parquet.fail-on-corrupted-statistics",
        "parquet.fail-on-corrupted-statistics",
        "parquet.optimized-reader.enabled",
        "parquet.optimized-nested-reader.enabled"
})
public class ParquetReaderConfig
{
    public static final String PARQUET_READER_MAX_SMALL_FILE_THRESHOLD = "15MB";

    private ParquetReaderOptions options = new ParquetReaderOptions();

    public boolean isIgnoreStatistics()
    {
        return options.isIgnoreStatistics();
    }

    @Config("parquet.ignore-statistics")
    @ConfigDescription("Ignore statistics from Parquet to allow querying files with corrupted or incorrect statistics")
    public ParquetReaderConfig setIgnoreStatistics(boolean ignoreStatistics)
    {
        options = options.withIgnoreStatistics(ignoreStatistics);
        return this;
    }

    @NotNull
    public DataSize getMaxReadBlockSize()
    {
        return options.getMaxReadBlockSize();
    }

    @Config("parquet.max-read-block-size")
    @LegacyConfig("hive.parquet.max-read-block-size")
    public ParquetReaderConfig setMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        options = options.withMaxReadBlockSize(maxReadBlockSize);
        return this;
    }

    @Min(128)
    @Max(65536)
    public int getMaxReadBlockRowCount()
    {
        return options.getMaxReadBlockRowCount();
    }

    @Config("parquet.max-read-block-row-count")
    @ConfigDescription("Maximum number of rows read in a batch")
    public ParquetReaderConfig setMaxReadBlockRowCount(int length)
    {
        options = options.withMaxReadBlockRowCount(length);
        return this;
    }

    @NotNull
    public DataSize getMaxMergeDistance()
    {
        return options.getMaxMergeDistance();
    }

    @Config("parquet.max-merge-distance")
    public ParquetReaderConfig setMaxMergeDistance(DataSize distance)
    {
        options = options.withMaxMergeDistance(distance);
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxBufferSize()
    {
        return options.getMaxBufferSize();
    }

    @Config("parquet.max-buffer-size")
    public ParquetReaderConfig setMaxBufferSize(DataSize size)
    {
        options = options.withMaxBufferSize(size);
        return this;
    }

    @Config("parquet.use-column-index")
    @ConfigDescription("Enable using Parquet column indexes")
    public ParquetReaderConfig setUseColumnIndex(boolean useColumnIndex)
    {
        options = options.withUseColumnIndex(useColumnIndex);
        return this;
    }

    public boolean isUseColumnIndex()
    {
        return options.isUseColumnIndex();
    }

    @Config("parquet.use-bloom-filter")
    @ConfigDescription("Use Parquet Bloom filters")
    public ParquetReaderConfig setUseBloomFilter(boolean useBloomFilter)
    {
        options = options.withBloomFilter(useBloomFilter);
        return this;
    }

    public boolean isUseBloomFilter()
    {
        return options.useBloomFilter();
    }

    @Config("parquet.small-file-threshold")
    @ConfigDescription("Size below which a parquet file will be read entirely")
    public ParquetReaderConfig setSmallFileThreshold(DataSize smallFileThreshold)
    {
        options = options.withSmallFileThreshold(smallFileThreshold);
        return this;
    }

    @NotNull
    @MaxDataSize(PARQUET_READER_MAX_SMALL_FILE_THRESHOLD)
    public DataSize getSmallFileThreshold()
    {
        return options.getSmallFileThreshold();
    }

    @Config("parquet.experimental.vectorized-decoding.enabled")
    @ConfigDescription("Enable using Java Vector API for faster decoding of parquet files")
    public ParquetReaderConfig setVectorizedDecodingEnabled(boolean vectorizedDecodingEnabled)
    {
        options = options.withVectorizedDecodingEnabled(vectorizedDecodingEnabled);
        return this;
    }

    public boolean isVectorizedDecodingEnabled()
    {
        return options.isVectorizedDecodingEnabled();
    }

    @Config("parquet.crypto-factory-class")
    @ConfigDescription("Crypto factory class to encrypt or decrypt parquet files")
    public ParquetReaderConfig setCryptoFactoryClass(String cryptoFactoryClass)
    {
        options = options.withEncryptionOption(options.encryptionOptions().withCryptoFactoryClass(cryptoFactoryClass));
        return this;
    }

    public String getCryptoFactoryClass()
    {
        return options.getCryptoFactoryClass();
    }

    @Config("parquet.encryption-kms-client-class")
    @ConfigDescription("Class implementing the KmsClient interface. KMS stands for “key management service")
    public ParquetReaderConfig setEncryptionKmsClientClass(String encryptionKmsClientClass)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionKmsClientClass(encryptionKmsClientClass));
        return this;
    }

    public String getEncryptionKmsClientClass()
    {
        return options.getEncryptionKmsClientClass();
    }

    @Config("parquet.encryption-kms-instance-id")
    @ConfigDescription("")
    public ParquetReaderConfig setEncryptionKmsInstanceId(String encryptionKmsInstanceId)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionKmsInstanceId(encryptionKmsInstanceId));
        return this;
    }

    public String getEncryptionKmsInstanceId()
    {
        return options.getEncryptionKmsInstanceId();
    }

    @Config("parquet.encryption-kms-instance-url")
    @ConfigDescription("")
    public ParquetReaderConfig setEncryptionKmsInstanceUrl(String encryptionKmsInstanceUrl)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionKmsInstanceUrl(encryptionKmsInstanceUrl));
        return this;
    }

    public String getEncryptionKmsInstanceUrl()
    {
        return options.getEncryptionKmsInstanceUrl();
    }

    @Config("parquet.encryption-key-access-token")
    @ConfigDescription("")
    public ParquetReaderConfig setEncryptionKeyAccessToken(String encryptionKeyAccessToken)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionKeyAccessToken(encryptionKeyAccessToken));
        return this;
    }

    public String getEncryptionKeyAccessToken()
    {
        return options.getEncryptionKeyAccessToken();
    }

    @Config("parquet.encryption-cache-lifetime-seconds")
    @ConfigDescription("")
    public ParquetReaderConfig setEncryptionCacheLifetimeSeconds(Long encryptionCacheLifetimeSeconds)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionCacheLifetimeSeconds(encryptionCacheLifetimeSeconds));
        return this;
    }

    public Long getEncryptionCacheLifetimeSeconds()
    {
        return options.getEncryptionCacheLifetimeSeconds();
    }

    public String getEncryptionMasterKeyFile()
    {
        return options.getEncryptionKeyFile();
    }

    @Config("parquet.encryption-master-key-file")
    @ConfigDescription("the path to master key file")
    public ParquetReaderConfig setEncryptionMasterKeyFile(String keyFile)
    {
        options = options.withEncryptionOption(
                options.encryptionOptions().withEncryptionKeyFile(keyFile));
        return this;
    }

    public ParquetReaderOptions toParquetReaderOptions()
    {
        return options;
    }
}
