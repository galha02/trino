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
package io.prestosql.plugin.hive.rubix;

import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.spi.CacheConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRubixConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RubixConfig.class)
                .setBookKeeperServerPort(CacheConfig.DEFAULT_BOOKKEEPER_SERVER_PORT)
                .setDataTransferServerPort(CacheConfig.DEFAULT_DATA_TRANSFER_SERVER_PORT)
                .setCacheLocation("/tmp")
                .setParallelWarmupEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.cache.parallel-warmup-enabled", "false")
                .put("hive.cache.location", "/etc")
                .put("hive.cache.rubix-bookkeeper-port", "1234")
                .put("hive.cache.rubix-data-transfer-port", "1235")
                .build();

        RubixConfig expected = new RubixConfig()
                .setParallelWarmupEnabled(false)
                .setCacheLocation("/etc")
                .setBookKeeperServerPort(1234)
                .setDataTransferServerPort(1235);

        assertFullMapping(properties, expected);
    }
}
