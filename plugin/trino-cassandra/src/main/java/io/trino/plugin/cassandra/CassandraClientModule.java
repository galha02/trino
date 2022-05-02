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
package io.trino.plugin.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_SSL_INITIALIZATION_FAILURE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CassandraClientModule
        implements Module
{
    private final TypeManager typeManager;

    public CassandraClientModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(CassandraConnector.class).in(Scopes.SINGLETON);
        binder.bind(CassandraMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTokenSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(CassandraClientConfig.class);

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(TypeId.of(value));
        }
    }

    @Singleton
    @Provides
    public static CassandraSession createCassandraSession(CassandraClientConfig config, JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        Cluster.Builder clusterBuilder = Cluster.builder();
        if (config.getProtocolVersion() != null) {
            clusterBuilder.withProtocolVersion(config.getProtocolVersion());
        }

        List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
        checkArgument(!contactPoints.isEmpty(), "empty contactPoints");
        clusterBuilder.withPort(config.getNativeProtocolPort());
        clusterBuilder.withReconnectionPolicy(new ExponentialReconnectionPolicy(500, 10000));
        clusterBuilder.withRetryPolicy(config.getRetryPolicy().getPolicy());

        LoadBalancingPolicy loadPolicy = new RoundRobinPolicy();

        if (config.isUseDCAware()) {
            requireNonNull(config.getDcAwareLocalDC(), "DCAwarePolicy localDC is null");
            DCAwareRoundRobinPolicy.Builder builder = DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(config.getDcAwareLocalDC());
            if (config.getDcAwareUsedHostsPerRemoteDc() > 0) {
                builder.withUsedHostsPerRemoteDc(config.getDcAwareUsedHostsPerRemoteDc());
                if (config.isDcAwareAllowRemoteDCsForLocal()) {
                    builder.allowRemoteDCsForLocalConsistencyLevel();
                }
            }
            loadPolicy = builder.build();
        }

        if (config.isUseTokenAware()) {
            loadPolicy = new TokenAwarePolicy(loadPolicy, config.isTokenAwareShuffleReplicas());
        }

        if (!config.getAllowedAddresses().isEmpty()) {
            checkArgument(!config.getAllowedAddresses().isEmpty(), "empty AllowListAddresses");
            List<InetSocketAddress> allowList = new ArrayList<>();
            for (String point : config.getAllowedAddresses()) {
                allowList.add(new InetSocketAddress(point, config.getNativeProtocolPort()));
            }
            loadPolicy = new WhiteListPolicy(loadPolicy, allowList);
        }

        clusterBuilder.withLoadBalancingPolicy(loadPolicy);

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(toIntExact(config.getClientReadTimeout().toMillis()));
        socketOptions.setConnectTimeoutMillis(toIntExact(config.getClientConnectTimeout().toMillis()));
        if (config.getClientSoLinger() != null) {
            socketOptions.setSoLinger(config.getClientSoLinger());
        }
        if (config.isTlsEnabled()) {
            buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                    .ifPresent(context -> clusterBuilder.withSSL(JdkSSLOptions.builder().withSSLContext(context).build()));
        }
        clusterBuilder.withSocketOptions(socketOptions);

        if (config.getUsername() != null && config.getPassword() != null) {
            clusterBuilder.withCredentials(config.getUsername(), config.getPassword());
        }

        QueryOptions options = new QueryOptions();
        options.setFetchSize(config.getFetchSize());
        options.setConsistencyLevel(config.getConsistencyLevel());
        clusterBuilder.withQueryOptions(options);

        if (config.getSpeculativeExecutionLimit().isPresent()) {
            clusterBuilder.withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(
                    config.getSpeculativeExecutionDelay().toMillis(), // delay before a new execution is launched
                    config.getSpeculativeExecutionLimit().get())); // maximum number of executions
        }

        return new CassandraSession(
                extraColumnMetadataCodec,
                new ReopeningCluster(() -> {
                    contactPoints.forEach(clusterBuilder::addContactPoint);
                    return clusterBuilder.build();
                }),
                config.getNoHostAvailableRetryTimeout());
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(CASSANDRA_SSL_INITIALIZATION_FAILURE, e);
        }
    }
}
