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
package io.trino.server.remotetask;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.prestosql.execution.TaskId;
import io.prestosql.server.DynamicFilterService;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.client.PrestoHeaders.PRESTO_CURRENT_VERSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static io.prestosql.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static java.util.Objects.requireNonNull;

class DynamicFiltersFetcher
        implements SimpleHttpResponseCallback<VersionedDynamicFilterDomains>
{
    private final TaskId taskId;
    private final URI taskUri;
    private final Consumer<Throwable> onFail;
    private final JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec;
    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;
    private final RemoteTaskStats stats;
    private final DynamicFilterService dynamicFilterService;
    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    @GuardedBy("this")
    private long dynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
    @GuardedBy("this")
    private long localDynamicFiltersVersion = INITIAL_DYNAMIC_FILTERS_VERSION;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private ListenableFuture<JsonResponse<VersionedDynamicFilterDomains>> future;

    public DynamicFiltersFetcher(
            Consumer<Throwable> onFail,
            TaskId taskId,
            URI taskUri,
            Duration refreshMaxWait,
            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec,
            Executor executor,
            HttpClient httpClient,
            Duration maxErrorDuration,
            ScheduledExecutorService errorScheduledExecutor,
            RemoteTaskStats stats,
            DynamicFilterService dynamicFilterService)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskUri = requireNonNull(taskUri, "taskUri is null");
        this.onFail = requireNonNull(onFail, "onFail is null");

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.dynamicFilterDomainsCodec = requireNonNull(dynamicFilterDomainsCodec, "dynamicFilterDomainsCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker = new RequestErrorTracker(taskId, taskUri, maxErrorDuration, errorScheduledExecutor, "getting dynamic filter domains");
        this.stats = requireNonNull(stats, "stats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
    }

    public synchronized void start()
    {
        if (running) {
            // already running
            return;
        }
        running = true;
        fetchDynamicFiltersIfNecessary();
    }

    public synchronized void stop()
    {
        running = false;
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    public synchronized void updateDynamicFiltersVersion(long newDynamicFiltersVersion)
    {
        if (dynamicFiltersVersion >= newDynamicFiltersVersion) {
            return;
        }

        dynamicFiltersVersion = newDynamicFiltersVersion;
        fetchDynamicFiltersIfNecessary();
    }

    private synchronized void fetchDynamicFiltersIfNecessary()
    {
        // stopped?
        if (!running) {
            return;
        }

        // local dynamic filters are up to date
        if (localDynamicFiltersVersion >= dynamicFiltersVersion) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::fetchDynamicFiltersIfNecessary, executor);
            return;
        }

        Request request = prepareGet()
                .setUri(uriBuilderFrom(taskUri).appendPath("dynamicfilters").build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(PRESTO_CURRENT_VERSION, Long.toString(localDynamicFiltersVersion))
                .setHeader(PRESTO_MAX_WAIT, refreshMaxWait.toString())
                .build();

        errorTracker.startRequest();
        future = httpClient.executeAsync(request, createFullJsonResponseHandler(dynamicFilterDomainsCodec));
        currentRequestStartNanos.set(System.nanoTime());
        addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri(), stats), executor);
    }

    @Override
    public void success(VersionedDynamicFilterDomains newDynamicFilterDomains)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                updateDynamicFilterDomains(newDynamicFilterDomains);
                errorTracker.requestSucceeded();
            }
            finally {
                fetchDynamicFiltersIfNecessary();
            }
        }
    }

    @Override
    public void failed(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            try {
                errorTracker.requestFailed(cause);
            }
            catch (Error e) {
                onFail.accept(e);
                throw e;
            }
            catch (RuntimeException e) {
                onFail.accept(e);
            }
            finally {
                fetchDynamicFiltersIfNecessary();
            }
        }
    }

    @Override
    public void fatal(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("DynamicFiltersFetcher-%s", taskId)) {
            updateStats(currentRequestStartNanos.get());
            onFail.accept(cause);
        }
    }

    private void updateDynamicFilterDomains(VersionedDynamicFilterDomains newDynamicFilterDomains)
    {
        synchronized (this) {
            if (localDynamicFiltersVersion >= newDynamicFilterDomains.getVersion()) {
                // newer dynamic filters were already received
                return;
            }

            localDynamicFiltersVersion = newDynamicFilterDomains.getVersion();
            updateDynamicFiltersVersion(localDynamicFiltersVersion);
        }

        // Subsequent DF versions can be narrowing down only. Therefore order in which they are intersected
        // (and passed to dynamic filter service) doesn't matter.
        dynamicFilterService.addTaskDynamicFilters(taskId, newDynamicFilterDomains.getDynamicFilterDomains());
    }

    private void updateStats(long currentRequestStartNanos)
    {
        stats.statusRoundTripMillis(nanosSince(currentRequestStartNanos).toMillis());
    }
}
