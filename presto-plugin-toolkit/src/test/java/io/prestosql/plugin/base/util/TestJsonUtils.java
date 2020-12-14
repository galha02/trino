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
package io.prestosql.plugin.base.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.prestosql.plugin.base.util.JsonUtils.parseJson;
import static io.prestosql.plugin.base.util.TestJsonUtils.TestEnum.OPTION_A;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonUtils
{
    enum TestEnum
    {
        OPTION_A,
        OPTION_B
    }

    public static class TestObject
    {
        @JsonProperty
        public TestEnum testEnum;
    }

    @Test
    public void testLowercaseEnum()
            throws IOException
    {
        TestObject parsed = parseJson("{\"testEnum\": \"option_a\"}".getBytes(US_ASCII), TestObject.class);
        assertThat(parsed.testEnum).isEqualTo(OPTION_A);
    }
}
