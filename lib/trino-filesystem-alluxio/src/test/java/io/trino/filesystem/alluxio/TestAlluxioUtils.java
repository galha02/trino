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
package io.trino.filesystem.alluxio;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAlluxioUtils
{
    @Test
    public void test()
    {
        String path = "test/level0-file0";
        Assertions.assertEquals(path, AlluxioUtils.simplifyPath(path));

        path = "a/./b/../../c/";
        Assertions.assertEquals("c/", AlluxioUtils.simplifyPath(path));
    }
}
