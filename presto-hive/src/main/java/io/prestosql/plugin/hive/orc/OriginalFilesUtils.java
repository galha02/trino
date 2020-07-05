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
package io.prestosql.plugin.hive.orc;

import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

import static io.prestosql.plugin.hive.AcidInfo.OriginalFileInfo;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;

public class OriginalFilesUtils
{
    private OriginalFilesUtils()
    {
    }

    /**
     * Returns number of rows present in the file, based on the ORC footer.
     */
    private static Long getRowsInFile(
            Path splitPath,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            OrcReaderOptions options,
            Configuration configuration,
            FileFormatDataSourceStats stats,
            long fileSize)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, splitPath, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(sessionUser, () -> fileSystem.open(splitPath));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(splitPath.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Could not create ORC data source for file: " + splitPath.getName(), e);
        }

        OrcReader reader;
        try {
            reader = new OrcReader(orcDataSource, options);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Could not create ORC reader for file: " + splitPath.getName(), e);
        }
        return reader.getFooter().getNumberOfRows();
    }

    /**
     * Returns total number of rows present before the given original file in the same bucket.
     * example: if bucket-1 has original files
     * 000000_0 -> X0 rows
     * 000000_0_copy1 -> X1 rows
     * 000000_0_copy2 -> X2 rows
     * <p>
     * for 000000_0_copy2, it returns (X0+X1)
     */
    public static long getRowCount(
            Collection<OriginalFileInfo> originalFileInfos,
            Path splitPath,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            OrcReaderOptions options,
            Configuration configuration,
            FileFormatDataSourceStats stats)
    {
        long rowCount = 0;
        for (OriginalFileInfo originalFileInfo : originalFileInfos) {
            Path path = new Path(splitPath.getParent() + "/" + originalFileInfo.getName());
            // Check if the file belongs to the same bucket and comes before 'reqPath' in lexicographic order.
            if (path.compareTo(splitPath) < 0) {
                rowCount += getRowsInFile(path, hdfsEnvironment, sessionUser, options, configuration, stats, originalFileInfo.getFileSize());
            }
        }

        return rowCount;
    }
}
