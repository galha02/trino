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
package io.trino.orc.stream;

import io.airlift.slice.Slice;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDecompressor;
import io.trino.orc.checkpoint.DoubleStreamCheckpoint;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcDecompressor.createOrcDecompressor;
import static io.trino.orc.metadata.CompressionKind.SNAPPY;

public class TestDoubleStream
        extends AbstractTestValueStream<Double, DoubleStreamCheckpoint, DoubleOutputStream, DoubleInputStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Double>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Double> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((double) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected DoubleOutputStream createValueOutputStream()
    {
        return new DoubleOutputStream(SNAPPY, COMPRESSION_BLOCK_SIZE);
    }

    @Override
    protected void writeValue(DoubleOutputStream outputStream, Double value)
    {
        outputStream.writeDouble(value);
    }

    @Override
    protected DoubleInputStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, COMPRESSION_BLOCK_SIZE, NATIVE_ZSTD_DECOMPRESSOR_ENABLED);
        return new DoubleInputStream(new OrcInputStream(OrcChunkLoader.create(ORC_DATA_SOURCE_ID, slice, orcDecompressor, newSimpleAggregatedMemoryContext())));
    }

    @Override
    protected Double readValue(DoubleInputStream valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
