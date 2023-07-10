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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.filesystem.Location;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LocationHandle
{
    private final Location targetPath;
    private final Location writePath;
    private final WriteMode writeMode;

    public LocationHandle(Location targetPath, Location writePath, WriteMode writeMode)
    {
        if (writeMode.isWritePathSameAsTargetPath() && !targetPath.equals(writePath)) {
            throw new IllegalArgumentException(format("targetPath is expected to be same as writePath for writeMode %s", writeMode));
        }
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.writeMode = requireNonNull(writeMode, "writeMode is null");
    }

    @JsonCreator
    public LocationHandle(
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("writePath") String writePath,
            @JsonProperty("writeMode") WriteMode writeMode)
    {
        this(
                Location.of(requireNonNull(targetPath, "targetPath is null")),
                Location.of(requireNonNull(writePath, "writePath is null")),
                writeMode);
    }

    // This method should only be called by LocationService
    Location getTargetPath()
    {
        return targetPath;
    }

    // This method should only be called by LocationService
    Location getWritePath()
    {
        return writePath;
    }

    // This method should only be called by LocationService
    public WriteMode getWriteMode()
    {
        return writeMode;
    }

    @JsonProperty("targetPath")
    public String getJsonSerializableTargetPath()
    {
        return targetPath.toString();
    }

    @JsonProperty("writePath")
    public String getJsonSerializableWritePath()
    {
        return writePath.toString();
    }

    @JsonProperty("writeMode")
    public WriteMode getJsonSerializableWriteMode()
    {
        return writeMode;
    }

    public enum WriteMode
    {
        /**
         * common mode for new table or existing table (both new and existing partition) and when staging directory is enabled
         */
        STAGE_AND_MOVE_TO_TARGET_DIRECTORY(false),
        /**
         * for new table in S3 or when staging directory is disabled
         */
        DIRECT_TO_TARGET_NEW_DIRECTORY(true),
        /**
         * for existing table in S3 (both new and existing partition) or when staging directory is disabled
         */
        DIRECT_TO_TARGET_EXISTING_DIRECTORY(true),
        /**/;

        // NOTE: Insert overwrite simulation (partition drops and partition additions in the same
        // transaction get merged and become one or more partition alterations, and get submitted to
        // metastore in close succession of each other) is not supported for S3. S3 uses the last
        // mode for insert into existing table. This is hard to support because the directory
        // containing the old data cannot be deleted until commit. Nor can the old data be moved
        // (assuming Hive HDFS directory naming convention shall not be violated). As a result,
        // subsequent insertion will have to write to directory belonging to existing partition.
        // This undermines the benefit of having insert overwrite simulation. This also makes
        // dropping of old partition at commit time hard because data added after the logical
        // "drop" time was added to the directories to be dropped.

        private final boolean writePathSameAsTargetPath;

        WriteMode(boolean writePathSameAsTargetPath)
        {
            this.writePathSameAsTargetPath = writePathSameAsTargetPath;
        }

        public boolean isWritePathSameAsTargetPath()
        {
            return writePathSameAsTargetPath;
        }
    }
}
