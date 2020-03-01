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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestRaptorPlugin
{
    @Test
    public void testPlugin()
            throws Exception
    {
        Plugin plugin = new RaptorPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, RaptorConnectorFactory.class);

        File tmpDir = Files.createTempDir();
        try {
            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("metadata.db.type", "h2")
                    .put("metadata.db.filename", tmpDir.getAbsolutePath())
                    .put("storage.data-directory", tmpDir.getAbsolutePath())
                    .build();

            factory.create("test", config, new TestingConnectorContext());
        }
        finally {
            deleteRecursively(tmpDir.toPath(), ALLOW_INSECURE);
        }
    }
}
