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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "elasticsearch.max-hits",
        "elasticsearch.cluster-name",
        "searchguard.ssl.certificate-format",
        "searchguard.ssl.pemcert-filepath",
        "searchguard.ssl.pemkey-filepath",
        "searchguard.ssl.pemkey-password",
        "searchguard.ssl.pemtrustedcas-filepath",
        "searchguard.ssl.keystore-filepath",
        "searchguard.ssl.keystore-password",
        "searchguard.ssl.truststore-filepath",
        "searchguard.ssl.truststore-password",
        "elasticsearch.table-description-directory",
        "elasticsearch.max-request-retries",
        "elasticsearch.max-request-retry-time"})
public class ElasticsearchConfig
{
    public static class SecurityOptions
    {
        static final String AWS = "AWS";
        static final String PASSWORD = "PASSWORD";
    }

    private List<String> hosts;
    private int port = 9200;
    private String defaultSchema = "default";
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, MINUTES);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private Duration connectTimeout = new Duration(1, SECONDS);
    private Duration backoffInitDelay = new Duration(500, MILLISECONDS);
    private Duration backoffMaxDelay = new Duration(20, SECONDS);
    private Duration maxRetryTime = new Duration(30, SECONDS);
    private Duration nodeRefreshInterval = new Duration(1, MINUTES);
    private int maxHttpConnections = 25;
    private int httpThreadCount = Runtime.getRuntime().availableProcessors();

    private boolean tlsEnabled;
    private File keystorePath;
    private File trustStorePath;
    private String keystorePassword;
    private String truststorePassword;
    private boolean ignorePublishAddress;
    private boolean verifyHostnames = true;

    private String security;

    @NotNull
    public List<String> getHosts()
    {
        return hosts;
    }

    @Config("elasticsearch.host")
    public ElasticsearchConfig setHosts(List<String> hosts)
    {
        this.hosts = hosts;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("elasticsearch.port")
    public ElasticsearchConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("elasticsearch.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public ElasticsearchConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    @Min(1)
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("elasticsearch.scroll-size")
    @ConfigDescription("Scroll batch size")
    public ElasticsearchConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("elasticsearch.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public ElasticsearchConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("elasticsearch.request-timeout")
    @ConfigDescription("Elasticsearch request timeout")
    public ElasticsearchConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("elasticsearch.connect-timeout")
    @ConfigDescription("Elasticsearch connect timeout")
    public ElasticsearchConfig setConnectTimeout(Duration timeout)
    {
        this.connectTimeout = timeout;
        return this;
    }

    @NotNull
    public Duration getBackoffInitDelay()
    {
        return backoffInitDelay;
    }

    @Config("elasticsearch.backoff-init-delay")
    @ConfigDescription("Initial delay to wait between backpressure retries")
    public ElasticsearchConfig setBackoffInitDelay(Duration backoffInitDelay)
    {
        this.backoffInitDelay = backoffInitDelay;
        return this;
    }

    @NotNull
    public Duration getBackoffMaxDelay()
    {
        return backoffMaxDelay;
    }

    @Config("elasticsearch.backoff-max-delay")
    @ConfigDescription("Maximum delay to wait between backpressure retries")
    public ElasticsearchConfig setBackoffMaxDelay(Duration backoffMaxDelay)
    {
        this.backoffMaxDelay = backoffMaxDelay;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("elasticsearch.max-retry-time")
    @ConfigDescription("Maximum timeout in case of multiple retries")
    public ElasticsearchConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getNodeRefreshInterval()
    {
        return nodeRefreshInterval;
    }

    @Config("elasticsearch.node-refresh-interval")
    @ConfigDescription("How often to refresh the list of available Elasticsearch nodes")
    public ElasticsearchConfig setNodeRefreshInterval(Duration nodeRefreshInterval)
    {
        this.nodeRefreshInterval = nodeRefreshInterval;
        return this;
    }

    @Config("elasticsearch.max-http-connections")
    @ConfigDescription("Maximum number of persistent HTTP connections to Elasticsearch")
    public ElasticsearchConfig setMaxHttpConnections(int size)
    {
        this.maxHttpConnections = size;
        return this;
    }

    @NotNull
    public int getMaxHttpConnections()
    {
        return maxHttpConnections;
    }

    @Config("elasticsearch.http-thread-count")
    @ConfigDescription("Number of threads handling HTTP connections to Elasticsearch")
    public ElasticsearchConfig setHttpThreadCount(int count)
    {
        this.httpThreadCount = count;
        return this;
    }

    @NotNull
    public int getHttpThreadCount()
    {
        return httpThreadCount;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("elasticsearch.tls.enabled")
    public ElasticsearchConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("elasticsearch.tls.keystore-path")
    public ElasticsearchConfig setKeystorePath(File path)
    {
        this.keystorePath = path;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("elasticsearch.tls.keystore-password")
    @ConfigSecuritySensitive
    public ElasticsearchConfig setKeystorePassword(String password)
    {
        this.keystorePassword = password;
        return this;
    }

    public Optional<@FileExists File> getTrustStorePath()
    {
        return Optional.ofNullable(trustStorePath);
    }

    @Config("elasticsearch.tls.truststore-path")
    public ElasticsearchConfig setTrustStorePath(File path)
    {
        this.trustStorePath = path;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("elasticsearch.tls.truststore-password")
    @ConfigSecuritySensitive
    public ElasticsearchConfig setTruststorePassword(String password)
    {
        this.truststorePassword = password;
        return this;
    }

    public boolean isVerifyHostnames()
    {
        return verifyHostnames;
    }

    @Config("elasticsearch.tls.verify-hostnames")
    public ElasticsearchConfig setVerifyHostnames(boolean verify)
    {
        this.verifyHostnames = verify;
        return this;
    }

    public boolean isIgnorePublishAddress()
    {
        return ignorePublishAddress;
    }

    @Config("elasticsearch.ignore-publish-address")
    public ElasticsearchConfig setIgnorePublishAddress(boolean ignorePublishAddress)
    {
        this.ignorePublishAddress = ignorePublishAddress;
        return this;
    }

    @NotNull
    public Optional<String> getSecurity()
    {
        return Optional.ofNullable(security);
    }

    @Config("elasticsearch.security")
    public ElasticsearchConfig setSecurity(String security)
    {
        this.security = security;
        return this;
    }
}
