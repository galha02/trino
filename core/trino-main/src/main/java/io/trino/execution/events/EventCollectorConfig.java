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

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.Min;

import static com.google.common.base.Preconditions.checkArgument;

public class EventCollectorConfig
{
    private int maxWarnings = Integer.MAX_VALUE;
    private int maxEvents = Integer.MAX_VALUE;

    @Config("event-collector.max-warnings")
    @LegacyConfig("warning-collector.max-warnings")
    public EventCollectorConfig setMaxWarnings(int maxWarnings)
    {
        checkArgument(maxWarnings >= 0, "maxWarnings must be >= 0");
        this.maxWarnings = maxWarnings;
        return this;
    }

    public int getMaxWarnings()
    {
        return maxWarnings;
    }

    @Config("event-collector.max-events")
    public EventCollectorConfig setMaxEvents(int maxEvents)
    {
        this.maxEvents = maxEvents;
        return this;
    }

    @Min(0)
    public int getMaxEvents()
    {
        return maxEvents;
    }
}
