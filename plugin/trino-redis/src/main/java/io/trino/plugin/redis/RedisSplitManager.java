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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific implementation of {@link ConnectorSplitManager}.
 */
public class RedisSplitManager
        implements ConnectorSplitManager
{
    private final Set<HostAddress> nodes;
    private final RedisJedisManager jedisManager;

    private static final long REDIS_MAX_SPLITS = 100;
    private static final long REDIS_STRIDE_SPLITS = 100;

    @Inject
    public RedisSplitManager(
            RedisConnectorConfig redisConnectorConfig,
            RedisJedisManager jedisManager)
    {
        requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
        this.nodes = ImmutableSet.copyOf(redisConnectorConfig.getNodes());
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        RedisTableHandle redisTableHandle = (RedisTableHandle) table;

        List<HostAddress> nodes = new ArrayList<>(this.nodes);
        Collections.shuffle(nodes);

        checkState(!nodes.isEmpty(), "No Redis nodes available");
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        long numberOfKeys = 1;
        // when Redis keys are provides in a zset, create multiple
        // splits by splitting zset in chunks
        if (redisTableHandle.keyDataFormat().equals("zset")) {
            try (Jedis jedis = jedisManager.getJedisPool(nodes.get(0)).getResource()) {
                numberOfKeys = jedis.zcount(redisTableHandle.keyName(), "-inf", "+inf");
            }
        }

        long stride = REDIS_STRIDE_SPLITS;

        if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
            stride = numberOfKeys / REDIS_MAX_SPLITS;
        }

        for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
            long endIndex = startIndex + stride - 1;
            if (endIndex >= numberOfKeys) {
                endIndex = -1;
            }

            RedisSplit split = new RedisSplit(
                    redisTableHandle.schemaName(),
                    redisTableHandle.tableName(),
                    redisTableHandle.keyDataFormat(),
                    redisTableHandle.valueDataFormat(),
                    redisTableHandle.keyName(),
                    redisTableHandle.constraint(),
                    startIndex,
                    endIndex,
                    nodes);

            builder.add(split);
        }
        return new FixedSplitSource(builder.build());
    }
}
