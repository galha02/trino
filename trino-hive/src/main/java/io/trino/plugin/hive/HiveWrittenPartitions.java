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
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorOutputMetadata;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveWrittenPartitions
        implements ConnectorOutputMetadata
{
    private final List<String> partitionNames;

    @JsonCreator
    public HiveWrittenPartitions(@JsonProperty("partitionNames") List<String> partitionNames)
    {
        this.partitionNames = ImmutableList.copyOf(requireNonNull(partitionNames, "partitionNames is null"));
    }

    @Override
    @JsonProperty
    public List<String> getInfo()
    {
        return partitionNames;
    }
}
