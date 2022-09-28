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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAwsSecurityConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AwsSecurityConfig.class)
                .setAccessKey(null)
                .setSecretKey(null)
                .setRegion(null)
                .setIamRole(null)
                .setExternalId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("elasticsearch.aws.access-key", "access")
                .put("elasticsearch.aws.secret-key", "secret")
                .put("elasticsearch.aws.region", "region")
                .put("elasticsearch.aws.iam-role", "iamRole")
                .put("elasticsearch.aws.external-id", "externalId")
                .buildOrThrow();

        AwsSecurityConfig expected = new AwsSecurityConfig()
                .setAccessKey("access")
                .setSecretKey("secret")
                .setRegion("region")
                .setIamRole("iamRole")
                .setExternalId("externalId");

        assertFullMapping(properties, expected);
    }
}
