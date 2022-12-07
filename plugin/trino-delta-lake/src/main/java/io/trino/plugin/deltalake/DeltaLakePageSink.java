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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;

public class DeltaLakePageSink
        extends AbstractDeltaLakePageSink
{
    public DeltaLakePageSink(
            List<DeltaLakeColumnHandle> inputColumns,
            List<String> originalPartitionColumns,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            TrinoFileSystemFactory fileSystemFactory,
            int maxOpenWriters,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            String outputPath,
            ConnectorSession session,
            DeltaLakeWriterStats stats,
            TypeManager typeManager,
            String trinoVersion)
    {
        super(
                inputColumns,
                originalPartitionColumns,
                pageIndexerFactory,
                hdfsEnvironment,
                fileSystemFactory,
                maxOpenWriters,
                dataFileInfoCodec,
                outputPath,
                session,
                stats,
                typeManager,
                trinoVersion);
    }

    @Override
    protected void processSynthesizedColumn(DeltaLakeColumnHandle column)
    {
        throw new IllegalStateException("Unexpected column type: " + column.getColumnType());
    }

    @Override
    protected void addSpecialColumns(
            List<DeltaLakeColumnHandle> inputColumns,
            ImmutableList.Builder<DeltaLakeColumnHandle> dataColumnHandles,
            ImmutableList.Builder<Integer> dataColumnsInputIndex,
            ImmutableList.Builder<String> dataColumnNames,
            ImmutableList.Builder<Type> dataColumnTypes) {}

    @Override
    protected String getRootTableLocation()
    {
        return outputPath;
    }

    @Override
    protected String getPartitionPrefixPath()
    {
        return "";
    }

    @Override
    protected Collection<Slice> buildResult()
    {
        return dataFileSlices.build();
    }

    @Override
    protected void collectDataFileInfo(DataFileInfo dataFileInfo)
    {
        dataFileSlices.add(wrappedBuffer(dataFileInfoCodec.toJsonBytes(dataFileInfo)));
    }
}
