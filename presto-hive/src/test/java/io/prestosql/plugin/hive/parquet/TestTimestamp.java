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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.benchmark.FileFormat;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.SqlTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSession;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTimestamp
{
    @Test
    public void testTimestampBackedByInt64()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_timestamp { optional int64 test (TIMESTAMP_MILLIS); }");
        ContiguousSet<Long> epochMillisValues = ContiguousSet.create(Range.closedOpen((long) -1_000, (long) 1_000), DiscreteDomain.longs());
        ImmutableList.Builder<SqlTimestamp> timestamps = new ImmutableList.Builder<>();
        for (long value : epochMillisValues) {
            timestamps.add(SqlTimestamp.fromMillis(3, value));
        }

        List<ObjectInspector> objectInspectors = singletonList(javaLongObjectInspector);
        List<String> columnNames = ImmutableList.of("test");

        ConnectorSession session = getHiveSession(new HiveConfig());

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf();
            jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);

            ParquetTester.writeParquetColumn(
                    jobConf,
                    tempFile.getFile(),
                    CompressionCodecName.SNAPPY,
                    ParquetTester.createTableProperties(columnNames, objectInspectors),
                    getStandardStructObjectInspector(columnNames, objectInspectors),
                    new Iterator<?>[] {epochMillisValues.iterator()},
                    Optional.of(parquetSchema),
                    false);

            Iterator<SqlTimestamp> expectedValues = timestamps.build().iterator();
            try (ConnectorPageSource pageSource = FileFormat.PRESTO_PARQUET.createFileFormatReader(session, HDFS_ENVIRONMENT, tempFile.getFile(), columnNames, ImmutableList.of(TIMESTAMP_MILLIS))) {
                // skip a page to exercise the decoder's skip() logic
                Page firstPage = pageSource.getNextPage();

                assertTrue(firstPage.getPositionCount() > 0, "Expected first page to have at least 1 row");

                for (int i = 0; i < firstPage.getPositionCount(); i++) {
                    expectedValues.next();
                }

                int pageCount = 1;
                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (page == null) {
                        continue;
                    }
                    pageCount++;
                    Block block = page.getBlock(0);

                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertThat(TIMESTAMP_MILLIS.getObjectValue(session, block, i))
                                .isEqualTo(expectedValues.next());
                    }
                }

                assertThat(pageCount)
                        .withFailMessage("Expected more than one page but processed %s", pageCount)
                        .isGreaterThan(1);
            }

            assertFalse(expectedValues.hasNext(), "Read fewer values than expected");
        }
    }
}
