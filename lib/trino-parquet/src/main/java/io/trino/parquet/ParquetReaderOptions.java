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
package io.trino.parquet;

import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class ParquetReaderOptions
{
    private static final DataSize DEFAULT_MAX_READ_BLOCK_SIZE = DataSize.of(16, MEGABYTE);
    private static final DataSize DEFAULT_MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize DEFAULT_MAX_BUFFER_SIZE = DataSize.of(8, MEGABYTE);

    private final boolean ignoreStatistics;
    private final DataSize maxReadBlockSize;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;

    public ParquetReaderOptions()
    {
        ignoreStatistics = false;
        maxReadBlockSize = DEFAULT_MAX_READ_BLOCK_SIZE;
        maxMergeDistance = DEFAULT_MAX_MERGE_DISTANCE;
        maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
    }

    private ParquetReaderOptions(
            boolean ignoreStatistics,
            DataSize maxReadBlockSize,
            DataSize maxMergeDistance,
            DataSize maxBufferSize)
    {
        this.ignoreStatistics = ignoreStatistics;
        this.maxReadBlockSize = requireNonNull(maxReadBlockSize, "maxMergeDistance is null");
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = requireNonNull(maxBufferSize, "maxBufferSize is null");
    }

    public boolean isIgnoreStatistics()
    {
        return ignoreStatistics;
    }

    public DataSize getMaxReadBlockSize()
    {
        return maxReadBlockSize;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    public ParquetReaderOptions withIgnoreStatistics(boolean ignoreStatistics)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxMergeDistance,
                maxBufferSize);
    }

    public ParquetReaderOptions withMaxReadBlockSize(DataSize maxReadBlockSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxMergeDistance,
                maxBufferSize);
    }

    public ParquetReaderOptions withMaxMergeDistance(DataSize maxMergeDistance)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxMergeDistance,
                maxBufferSize);
    }

    public ParquetReaderOptions withMaxBufferSize(DataSize maxBufferSize)
    {
        return new ParquetReaderOptions(
                ignoreStatistics,
                maxReadBlockSize,
                maxMergeDistance,
                maxBufferSize);
    }
}
