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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class DeltaLakeOutputInfo
{
    private final boolean partitioned;

    @JsonCreator
    public DeltaLakeOutputInfo(@JsonProperty("partitioned") boolean partitioned)
    {
        this.partitioned = partitioned;
    }

    @JsonProperty
    public boolean isPartitioned()
    {
        return partitioned;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeltaLakeOutputInfo that)) {
            return false;
        }
        return partitioned == that.partitioned;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioned);
    }
}
