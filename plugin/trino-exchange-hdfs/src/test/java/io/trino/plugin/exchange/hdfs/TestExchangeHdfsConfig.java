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
package io.trino.plugin.exchange.hdfs;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestExchangeHdfsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeHdfsConfig.class)
                .setHdfsProxyUser(null)
                .setHdfsStorageBlockSize(DataSize.of(4, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.hdfs.proxy-user", "work")
                .put("exchange.hdfs.block-size", "8MB")
                .buildOrThrow();

        ExchangeHdfsConfig expected = new ExchangeHdfsConfig()
                .setHdfsProxyUser("work")
                .setHdfsStorageBlockSize(DataSize.of(8, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
