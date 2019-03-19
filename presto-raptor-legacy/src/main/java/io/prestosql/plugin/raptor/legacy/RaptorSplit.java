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
package io.prestosql.plugin.raptor.legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private final Set<UUID> shardUuids;
    private final OptionalInt bucketNumber;
    private final List<HostAddress> addresses;
    private final TupleDomain<RaptorColumnHandle> effectivePredicate;
    private final OptionalLong transactionId;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("shardUuids") Set<UUID> shardUuids,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber,
            @JsonProperty("effectivePredicate") TupleDomain<RaptorColumnHandle> effectivePredicate,
            @JsonProperty("transactionId") OptionalLong transactionId)
    {
        this(shardUuids, bucketNumber, ImmutableList.of(), effectivePredicate, transactionId);
    }

    public RaptorSplit(
            UUID shardUuid,
            List<HostAddress> addresses,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId)
    {
        this(ImmutableSet.of(shardUuid), OptionalInt.empty(), addresses, effectivePredicate, transactionId);
    }

    public RaptorSplit(
            Set<UUID> shardUuids,
            int bucketNumber,
            HostAddress address,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId)
    {
        this(shardUuids, OptionalInt.of(bucketNumber), ImmutableList.of(address), effectivePredicate, transactionId);
    }

    private RaptorSplit(
            Set<UUID> shardUuids,
            OptionalInt bucketNumber,
            List<HostAddress> addresses,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            OptionalLong transactionId)
    {
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuid is null"));
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public Set<UUID> getShardUuids()
    {
        return shardUuids;
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @JsonProperty
    public TupleDomain<RaptorColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuids", shardUuids)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("hosts", addresses)
                .omitNullValues()
                .toString();
    }
}
