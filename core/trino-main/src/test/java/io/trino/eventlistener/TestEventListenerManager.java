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
package io.trino.eventlistener;

import com.google.common.collect.ImmutableMap;
import io.trino.server.configuration.ConfigurationResolver;
import io.trino.spi.eventlistener.EventListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class TestEventListenerManager
{
    @Test
    public void testShutdownIsForwardedToListeners()
    {
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig(), new ConfigurationResolver(ImmutableMap.of()));
        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListener listener = new EventListener()
        {
            @Override
            public void shutdown()
            {
                wasCalled.set(true);
            }
        };

        eventListenerManager.addEventListener(listener);
        eventListenerManager.loadEventListeners();
        eventListenerManager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }
}
