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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.prestosql.plugin.hive.HiveCompressionCodec.GZIP;
import static io.prestosql.plugin.iceberg.IcebergFileFormat.ORC;
import static io.prestosql.plugin.iceberg.IcebergFileFormat.PARQUET;

public class TestIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergConfig.class)
                .setFileFormat(ORC)
                .setCompressionCodec(GZIP)
                .setPurgeDataOnTableDrop(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("iceberg.file-format", "Parquet")
                .put("iceberg.compression-codec", "NONE")
                .put("iceberg.delete-files-on-table-drop", "true")
                .build();

        IcebergConfig expected = new IcebergConfig()
                .setFileFormat(PARQUET)
                .setCompressionCodec(HiveCompressionCodec.NONE)
                .setPurgeDataOnTableDrop(true);

        assertFullMapping(properties, expected);
    }
}
