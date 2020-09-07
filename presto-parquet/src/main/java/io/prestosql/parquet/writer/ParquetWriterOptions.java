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
package io.prestosql.parquet.writer;

import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.ParquetWriter;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetWriterOptions
{
    private static final DataSize DEFAULT_MAX_ROW_GROUP_SIZE = DataSize.ofBytes(ParquetWriter.DEFAULT_BLOCK_SIZE);
    private static final DataSize DEFAULT_MAX_PAGE_SIZE = DataSize.ofBytes(ParquetWriter.DEFAULT_PAGE_SIZE);

    public static ParquetWriterOptions.Builder builder()
    {
        return new ParquetWriterOptions.Builder();
    }

    private final int maxRowGroupSize;
    private final int maxPageSize;

    private ParquetWriterOptions(DataSize maxBlockSize, DataSize maxPageSize)
    {
        this.maxRowGroupSize = toIntExact(requireNonNull(maxBlockSize, "maxRowGroupSize is null").toBytes());
        this.maxPageSize = toIntExact(requireNonNull(maxPageSize, "maxPageSize is null").toBytes());
    }

    public long getMaxRowGroupSize()
    {
        return maxRowGroupSize;
    }

    public int getMaxPageSize()
    {
        return maxPageSize;
    }

    public static class Builder
    {
        private DataSize maxBlockSize = DEFAULT_MAX_ROW_GROUP_SIZE;
        private DataSize maxPageSize = DEFAULT_MAX_PAGE_SIZE;

        public Builder setMaxBlockSize(DataSize maxBlockSize)
        {
            this.maxBlockSize = maxBlockSize;
            return this;
        }

        public Builder setMaxPageSize(DataSize maxPageSize)
        {
            this.maxPageSize = maxPageSize;
            return this;
        }

        public ParquetWriterOptions build()
        {
            return new ParquetWriterOptions(maxBlockSize, maxPageSize);
        }
    }
}
