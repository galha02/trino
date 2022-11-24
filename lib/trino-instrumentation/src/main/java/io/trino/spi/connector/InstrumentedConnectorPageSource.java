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
package io.trino.spi.connector;

import io.trino.instrumentation.events.ConnectorPageSourceEvent;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * This class has been generated by the InstrumentationGenerator class.
 * The generation is a one-time event and is not repeated during build.
 */
@SuppressWarnings({"DeprecatedApi", "ExperimentalSpi", "DuplicatedCode"})
public final class InstrumentedConnectorPageSource
        implements ConnectorPageSource
{
    private final String catalogName;

    private final String delegateClassName;

    private final ConnectorPageSource delegate;

    private InstrumentedConnectorPageSource(String catalogName, ConnectorPageSource delegate)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateClassName = delegate.getClass().getSimpleName();
    }

    public static ConnectorPageSource instrument(ConnectorPageSource delegate)
    {
        if (delegate instanceof InstrumentedConnectorPageSource instrumented) {
            return instrumented;
        }
        return new InstrumentedConnectorPageSource("unknown", delegate);
    }

    public static ConnectorPageSource instrument(String catalogName, ConnectorPageSource delegate)
    {
        if (delegate instanceof InstrumentedConnectorPageSource instrumented) {
            return instrumented;
        }
        return new InstrumentedConnectorPageSource(catalogName, delegate);
    }

    @Override
    public void close() throws IOException
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "close";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            delegate.close();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "isFinished";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.isFinished();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "isBlocked";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.isBlocked();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public Metrics getMetrics()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getMetrics";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getMetrics();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getCompletedBytes";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getCompletedBytes();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public long getMemoryUsage()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getMemoryUsage";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getMemoryUsage();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getCompletedPositions";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getCompletedPositions();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getReadTimeNanos";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getReadTimeNanos();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }

    @Override
    public Page getNextPage()
    {
        final ConnectorPageSourceEvent event = new ConnectorPageSourceEvent();
        if (event.isEnabled()) {
            event.className = delegateClassName;
            event.method = "getNextPage";
            event.catalogName = catalogName;
        }
        event.begin();
        try {
            return delegate.getNextPage();
        }
        finally {
            event.end();
            if (event.shouldCommit()) {
                event.commit();
            }
        }
    }
}
