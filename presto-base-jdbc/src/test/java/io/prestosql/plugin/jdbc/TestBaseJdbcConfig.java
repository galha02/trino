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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.BaseJdbcConfig.LegacyGenericColumnMapping;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setJdbcTypesMappedToVarchar("")
                .setLegacyGenericColumnMapping(LegacyGenericColumnMapping.ENABLE)
                .setMetadataCacheTtl(Duration.valueOf("0m"))
                .setCacheMissing(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("jdbc-types-mapped-to-varchar", "mytype,struct_type1")
                .put("legacy-generic-column-mapping", "IGNORE")
                .put("metadata.cache-ttl", "1s")
                .put("metadata.cache-missing", "true")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setJdbcTypesMappedToVarchar("mytype, struct_type1")
                .setLegacyGenericColumnMapping(LegacyGenericColumnMapping.IGNORE)
                .setMetadataCacheTtl(Duration.valueOf("1s"))
                .setCacheMissing(true);

        assertFullMapping(properties, expected);

        assertEquals(expected.getJdbcTypesMappedToVarchar(), ImmutableSet.of("mytype", "struct_type1"));
    }
}
