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
package io.trino.server.testing;

import com.google.inject.Key;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.connector.InMemoryCatalogStore;
import io.trino.connector.StaticCatalogManager;
import io.trino.connector.WorkerDynamicCatalogManager;
import io.trino.metadata.CatalogManager;
import io.trino.spi.catalog.CatalogStore;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.connector.CatalogManagerConfig.CatalogMangerKind.STATIC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTestingTrinoServer
{
    @Test
    public void testDefaultCatalogManagement()
            throws IOException
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder().build()) {
            if (server.isCoordinator()) {
                assertThat(server.getInstance(Key.get(CatalogManager.class)))
                        .isInstanceOf(CoordinatorDynamicCatalogManager.class);
                assertThat(server.getInstance(Key.get(CatalogStore.class)))
                        .isInstanceOf(InMemoryCatalogStore.class);
            }
            else {
                assertThat(server.getInstance(Key.get(CatalogManager.class)))
                        .isInstanceOf(WorkerDynamicCatalogManager.class);
            }
        }
    }

    @Test
    public void testSetCatalogManagementToStatic()
            throws IOException
    {
        try (TestingTrinoServer server = TestingTrinoServer.builder()
                .setCatalogMangerKind(STATIC)
                .build()) {
            assertThat(server.getInstance(Key.get(CatalogManager.class)))
                    .isInstanceOf(StaticCatalogManager.class);
        }
    }
}
