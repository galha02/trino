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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class PrometheusConnectorConfig
{
    private URI prometheusURI = URI.create("http://localhost:9090");
    private Duration queryChunkSizeDuration = new Duration(1, TimeUnit.DAYS);
    private Duration maxQueryRangeDuration = new Duration(21, TimeUnit.DAYS);
    private Duration cacheDuration = new Duration(30, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(10, TimeUnit.SECONDS);
    private File bearerTokenFile;

    @NotNull
    public URI getPrometheusURI()
    {
        return prometheusURI;
    }

    @Config("prometheus.uri")
    @ConfigDescription("Where to find Prometheus coordinator host")
    public PrometheusConnectorConfig setPrometheusURI(URI prometheusURI)
    {
        this.prometheusURI = prometheusURI;
        return this;
    }

    @MinDuration("1ms")
    public Duration getQueryChunkSizeDuration()
    {
        return queryChunkSizeDuration;
    }

    @Config("prometheus.query.chunk.size.duration")
    @ConfigDescription("The duration of each query to Prometheus")
    public PrometheusConnectorConfig setQueryChunkSizeDuration(Duration queryChunkSizeDuration)
    {
        this.queryChunkSizeDuration = queryChunkSizeDuration;
        return this;
    }

    @MinDuration("1ms")
    public Duration getMaxQueryRangeDuration()
    {
        return maxQueryRangeDuration;
    }

    @Config("prometheus.max.query.range.duration")
    @ConfigDescription("Width of overall query to Prometheus, will be divided into prometheus.query.chunk.size.duration queries")
    public PrometheusConnectorConfig setMaxQueryRangeDuration(Duration maxQueryRangeDuration)
    {
        this.maxQueryRangeDuration = maxQueryRangeDuration;
        return this;
    }

    @MinDuration("1s")
    public Duration getCacheDuration()
    {
        return cacheDuration;
    }

    @Config("prometheus.cache.ttl")
    @ConfigDescription("How long values from this config file are cached")
    public PrometheusConnectorConfig setCacheDuration(Duration cacheConfigDuration)
    {
        this.cacheDuration = cacheConfigDuration;
        return this;
    }

    public Optional<File> getBearerTokenFile()
    {
        return Optional.ofNullable(bearerTokenFile);
    }

    @Config("prometheus.bearer.token.file")
    @ConfigDescription("File holding bearer token if needed for access to Prometheus")
    public PrometheusConnectorConfig setBearerTokenFile(File bearerTokenFile)
    {
        this.bearerTokenFile = bearerTokenFile;
        return this;
    }

    @MinDuration("1s")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("prometheus.read-timeout")
    @ConfigDescription("How much time a query to Prometheus has before timing out")
    public PrometheusConnectorConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @PostConstruct
    public void checkConfig()
    {
        long maxQueryRangeDuration = (long) getMaxQueryRangeDuration().getValue(TimeUnit.SECONDS);
        long queryChunkSizeDuration = (long) getQueryChunkSizeDuration().getValue(TimeUnit.SECONDS);
        if (maxQueryRangeDuration < queryChunkSizeDuration) {
            throw new ConfigurationException(ImmutableList.of(new Message("prometheus.max.query.range.duration must be greater than prometheus.query.chunk.size.duration")));
        }
    }
}
