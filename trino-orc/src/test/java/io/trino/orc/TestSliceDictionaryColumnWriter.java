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
package io.trino.orc;

import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.statistics.NoOpBloomFilterBuilder;
import io.trino.orc.metadata.statistics.StringStatisticsBuilder;
import io.trino.orc.writer.SliceDictionaryColumnWriter;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static io.trino.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;

public class TestSliceDictionaryColumnWriter
{
    @Test
    public void testDirectConversion()
    {
        SliceDictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                ROOT_COLUMN,
                VARCHAR,
                CompressionKind.NONE,
                toIntExact(DEFAULT_MAX_COMPRESSION_BUFFER_SIZE.toBytes()),
                () -> new StringStatisticsBuilder(toIntExact(DEFAULT_MAX_STRING_STATISTICS_LIMIT.toBytes()), new NoOpBloomFilterBuilder()));

        // a single row group exceeds 2G after direct conversion
        byte[] value = new byte[megabytes(1)];
        ThreadLocalRandom.current().nextBytes(value);
        Block data = RunLengthEncodedBlock.create(VARCHAR, Slices.wrappedBuffer(value), 3000);
        writer.beginRowGroup();
        writer.writeBlock(data);
        writer.finishRowGroup();

        assertFalse(writer.tryConvertToDirect(megabytes(64)).isPresent());
    }

    private static int megabytes(int size)
    {
        return toIntExact(DataSize.of(size, MEGABYTE).toBytes());
    }
}
