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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Stores information about Acid properties of a partition.
 */
public class AcidInfo
{
    private final String partitionLocation;
    private final List<DeleteDeltaInfo> deleteDeltas;
    private final List<OriginalFileInfo> originalFiles;
    private final int bucketId;

    @JsonCreator
    public AcidInfo(
            @JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<DeleteDeltaInfo> deleteDeltas,
            @JsonProperty("originalFiles") List<OriginalFileInfo> originalFiles,
            @JsonProperty("bucketId") int bucketId)
    {
        this.partitionLocation = requireNonNull(partitionLocation, "partitionLocation is null");
        this.deleteDeltas = ImmutableList.copyOf(requireNonNull(deleteDeltas, "deleteDeltas is null"));
        this.originalFiles = ImmutableList.copyOf(requireNonNull(originalFiles, "originalFiles is null"));
        this.bucketId = bucketId;
    }

    @JsonProperty
    public List<OriginalFileInfo> getOriginalFiles()
    {
        return originalFiles;
    }

    @JsonProperty
    public int getBucketId()
    {
        return bucketId;
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<DeleteDeltaInfo> getDeleteDeltas()
    {
        return deleteDeltas;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcidInfo that = (AcidInfo) o;
        return bucketId == that.bucketId &&
                Objects.equals(partitionLocation, that.partitionLocation) &&
                Objects.equals(deleteDeltas, that.deleteDeltas) &&
                Objects.equals(originalFiles, that.originalFiles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionLocation, deleteDeltas, originalFiles, bucketId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionLocation", partitionLocation)
                .add("deleteDeltas", deleteDeltas)
                .add("originalFiles", originalFiles)
                .add("bucketId", bucketId)
                .toString();
    }

    public static class DeleteDeltaInfo
    {
        private final String directoryName;

        @JsonCreator
        public DeleteDeltaInfo(@JsonProperty("directoryName") String directoryName)
        {
            this.directoryName = directoryName;
        }

        @JsonProperty
        public String getDirectoryName()
        {
            return directoryName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeleteDeltaInfo that = (DeleteDeltaInfo) o;
            return Objects.equals(directoryName, that.directoryName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(directoryName);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("directoryName", directoryName)
                    .toString();
        }
    }

    public static class OriginalFileInfo
    {
        private final String name;
        private final long fileSize;

        @JsonCreator
        public OriginalFileInfo(
                @JsonProperty("name") String name,
                @JsonProperty("fileSize") long fileSize)
        {
            this.name = requireNonNull(name, "name is null");
            this.fileSize = fileSize;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public long getFileSize()
        {
            return fileSize;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OriginalFileInfo that = (OriginalFileInfo) o;
            return fileSize == that.fileSize &&
                    name.equals(that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, fileSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("fileSize", fileSize)
                    .toString();
        }
    }

    public static Builder builder(Path partitionPath)
    {
        return new Builder(partitionPath);
    }

    public static Builder builder(AcidInfo acidInfo)
    {
        return new Builder(acidInfo);
    }

    public static class Builder
    {
        private final Path partitionLocation;
        private final List<DeleteDeltaInfo> deleteDeltaInfos = new ArrayList<>();
        private final ListMultimap<Integer, OriginalFileInfo> bucketIdToOriginalFileInfoMap = ArrayListMultimap.create();

        private Builder(Path partitionPath)
        {
            partitionLocation = requireNonNull(partitionPath, "partitionPath is null");
        }

        private Builder(AcidInfo acidInfo)
        {
            partitionLocation = new Path(acidInfo.getPartitionLocation());
            deleteDeltaInfos.addAll(acidInfo.deleteDeltas);
        }

        public Builder addDeleteDelta(Path deleteDeltaPath)
        {
            requireNonNull(deleteDeltaPath, "deleteDeltaPath is null");
            Path partitionPathFromDeleteDelta = deleteDeltaPath.getParent();
            checkArgument(
                    partitionLocation.equals(partitionPathFromDeleteDelta),
                    "Partition location in DeleteDelta '%s' does not match stored location '%s'",
                    deleteDeltaPath.getParent().toString(),
                    partitionLocation);

            deleteDeltaInfos.add(new DeleteDeltaInfo(deleteDeltaPath.getName()));
            return this;
        }

        public Builder addOriginalFile(Path originalFilePath, long originalFileLength, int bucketId)
        {
            requireNonNull(originalFilePath, "originalFilePath is null");
            Path partitionPathFromOriginalPath = originalFilePath.getParent();
            // originalFilePath has scheme in the prefix (i.e. scheme://<path>), extract path from uri and compare.
            checkArgument(
                    partitionLocation.toUri().getPath().equals(partitionPathFromOriginalPath.toUri().getPath()),
                    "Partition location in OriginalFile '%s' does not match stored location '%s'",
                    originalFilePath.getParent().toString(),
                    partitionLocation);
            bucketIdToOriginalFileInfoMap.put(bucketId, new OriginalFileInfo(originalFilePath.getName(), originalFileLength));
            return this;
        }

        public AcidInfo buildWithRequiredOriginalFiles(int bucketId)
        {
            checkState(
                    bucketId > -1 && bucketIdToOriginalFileInfoMap.containsKey(bucketId),
                    "Bucket Id to OriginalFileInfo map should have entry for requested bucket id: %s",
                    bucketId);
            List<DeleteDeltaInfo> deleteDeltas = ImmutableList.copyOf(deleteDeltaInfos);
            return new AcidInfo(partitionLocation.toString(), deleteDeltas, bucketIdToOriginalFileInfoMap.get(bucketId), bucketId);
        }

        public Optional<AcidInfo> build()
        {
            List<DeleteDeltaInfo> deleteDeltas = ImmutableList.copyOf(deleteDeltaInfos);
            if (deleteDeltas.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new AcidInfo(partitionLocation.toString(), deleteDeltas, ImmutableList.of(), -1));
        }
    }
}
