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
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.prestosql.plugin.jdbc.credential.CredentialProviderType.FILE;
import static io.prestosql.plugin.jdbc.credential.CredentialProviderType.INLINE;

public class TestBaseJdbcAuthenticationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BaseJdbcAuthenticationConfig.class)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCredentialProviderType(INLINE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("user-credential-name", "foo")
                .put("password-credential-name", "bar")
                .put("credential-provider.type", "FILE")
                .build();

        BaseJdbcAuthenticationConfig expected = new BaseJdbcAuthenticationConfig()
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCredentialProviderType(FILE);

        assertFullMapping(properties, expected);
    }
}
