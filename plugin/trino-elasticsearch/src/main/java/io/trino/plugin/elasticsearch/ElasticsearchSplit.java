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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record ElasticsearchSplit(
        String index,
        int shard,
        Optional<String> address)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ElasticsearchSplit.class);

    public ElasticsearchSplit
    {
        requireNonNull(index, "index is null");
        requireNonNull(address, "address is null");
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    @Override
    public List<HostAddress> getAddresses()
    {
        return address.map(host -> ImmutableList.of(HostAddress.fromString(host)))
                .orElseGet(ImmutableList::of);
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("index", index)
                .put("address", address.orElse(""))
                .buildOrThrow();
    }

    @JsonIgnore // TODO remove after https://github.com/airlift/airlift/pull/1141
    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(index)
                + sizeOf(address, SizeOf::estimatedSizeOf);
    }
}
