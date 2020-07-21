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
package io.prestosql.plugin.hive;

import com.google.common.io.Resources;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.hive.orc.OriginalFilesUtils;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.plugin.hive.AcidInfo.OriginalFileInfo;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static org.testng.Assert.assertTrue;

public class TestOriginalFilesUtils
{
    private String tablePath;
    private Configuration config;
    private final ConnectorSession session = TestingConnectorSession.SESSION;

    @BeforeClass
    public void setup()
    {
        tablePath = Resources.getResource(("dummy_id_data_orc")).getPath();
        config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    @Test
    public void testGetRowCountSingleOriginalFileBucket()
    {
        List<OriginalFileInfo> originalFileInfoList = new ArrayList<>();
        originalFileInfoList.add(new OriginalFileInfo("000001_0", 730));

        long rowCountResult = OriginalFilesUtils.getRowCount(originalFileInfoList,
                new Path(tablePath + "/000001_0"),
                HDFS_ENVIRONMENT,
                session.getUser(),
                new OrcReaderOptions(),
                config,
                new FileFormatDataSourceStats());
        assertTrue(rowCountResult == 0, "Original file should have 0 as the starting row count");
    }

    @Test
    public void testGetRowCountMultipleOriginalFilesBucket()
    {
        List<OriginalFileInfo> originalFileInfos = new ArrayList<>();

        originalFileInfos.add(new OriginalFileInfo("000002_0", 741));
        originalFileInfos.add(new OriginalFileInfo("000002_0_copy_1", 768));
        originalFileInfos.add(new OriginalFileInfo("000002_0_copy_2", 743));

        long rowCountResult = OriginalFilesUtils.getRowCount(originalFileInfos,
                new Path(tablePath + "/000002_0_copy_2"),
                HDFS_ENVIRONMENT,
                session.getUser(),
                new OrcReaderOptions(),
                config,
                new FileFormatDataSourceStats());
        // Bucket-2 has original files: 000002_0, 000002_0_copy_1. Each file original file has 4 rows.
        // So, starting row ID of 000002_0_copy_2 = row count of original files in Bucket-2 before it in lexicographic order.
        assertTrue(rowCountResult == 8, "Original file 000002_0_copy_2 should have 8 as the starting row count");
    }
}
