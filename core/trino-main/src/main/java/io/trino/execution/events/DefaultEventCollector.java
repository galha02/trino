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
package io.trino.execution.events;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoEvent;
import io.trino.spi.TrinoWarning;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultEventCollector
        implements EventCollector
{
    @GuardedBy("this")
    private final Set<TrinoWarning> warnings = new LinkedHashSet<>();

    @GuardedBy("this")
    private final Set<TrinoEvent> events = new LinkedHashSet<>();

    private final EventCollectorConfig config;

    public DefaultEventCollector(EventCollectorConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public synchronized void add(TrinoWarning warning)
    {
        requireNonNull(warning, "warning is null");
        if (warnings.size() < config.getMaxWarnings()) {
            warnings.add(warning);
        }
    }

    @Override
    public synchronized List<TrinoWarning> getWarnings()
    {
        return ImmutableList.copyOf(warnings);
    }

    @Override
    public synchronized void add(TrinoEvent event)
    {
        requireNonNull(events, "event is null");
        if (events.size() < config.getMaxEvents()) {
            events.add(event);
        }
    }

    @Override
    public synchronized List<TrinoEvent> getEvents()
    {
        return ImmutableList.copyOf(events);
    }
}
