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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TokenFetchingMetastoreClientFactory
        implements IdentityAwareMetastoreClientFactory
{
    private final TokenAwareMetastoreClientFactory clientProvider;
    private final boolean impersonationEnabled;
    private final NonEvictableLoadingCache<String, String> delegationTokenCache;

    @Inject
    public TokenFetchingMetastoreClientFactory(
            TokenAwareMetastoreClientFactory tokenAwareMetastoreClientFactory,
            ThriftMetastoreConfig thriftConfig)
    {
        this.clientProvider = requireNonNull(tokenAwareMetastoreClientFactory, "tokenAwareMetastoreClientFactory is null");
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();

        this.delegationTokenCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(thriftConfig.getDelegationTokenCacheTtl().toMillis(), MILLISECONDS)
                        .maximumSize(thriftConfig.getDelegationTokenCacheMaximumSize()),
                CacheLoader.from(this::loadDelegationToken));
    }

    private ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        return clientProvider.createMetastoreClient(Optional.empty());
    }

    @Override
    public ThriftMetastoreClient createMetastoreClientFor(Optional<ConnectorIdentity> identity)
            throws TException
    {
        if (!impersonationEnabled) {
            return createMetastoreClient();
        }

        String username = identity.map(ConnectorIdentity::getUser)
                .orElseThrow(() -> new IllegalStateException("End-user name should exist when metastore impersonation is enabled"));

        String delegationToken;
        try {
            delegationToken = delegationTokenCache.getUnchecked(username);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        return clientProvider.createMetastoreClient(Optional.of(delegationToken));
    }

    private String loadDelegationToken(String username)
    {
        try (ThriftMetastoreClient client = createMetastoreClient()) {
            return client.getDelegationToken(username);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
