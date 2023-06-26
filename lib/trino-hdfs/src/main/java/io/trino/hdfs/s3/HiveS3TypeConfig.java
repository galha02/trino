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
package io.trino.hdfs.s3;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class HiveS3TypeConfig
{
    private S3FileSystemType s3FileSystemType = S3FileSystemType.TRINO;

    @NotNull
    public S3FileSystemType getS3FileSystemType()
    {
        return s3FileSystemType;
    }

    @Config("hive.s3-file-system-type")
    public HiveS3TypeConfig setS3FileSystemType(S3FileSystemType s3FileSystemType)
    {
        this.s3FileSystemType = s3FileSystemType;
        return this;
    }
}
