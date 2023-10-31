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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestConnectorEventListener
{
    private final EventsCollector generatedEvents = new EventsCollector();

    private Closer closer;
    private EventsAwaitingQueries queries;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        closer = Closer.create();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).setNodeCount(1).build();
        closer.register(queryRunner);

        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withEventListener(new TestingEventListener(generatedEvents))
                        .build());
            }
        });
        queryRunner.createCatalog("mock-catalog", "mock");

        queryRunner.getCoordinator().addConnectorEventListeners();
        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        if (closer != null) {
            closer.close();
        }
        closer = null;
    }

    @Test
    public void testConnectorEventHandlerReceivingEvents()
            throws Exception
    {
        queries.runQueryAndWaitForEvents("SELECT 1", TEST_SESSION).getQueryEvents();
    }
}
