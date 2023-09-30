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
package io.trino.spi.block;

import org.junit.jupiter.api.Test;

import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBlockUtil
{
    @Test
    public void testCalculateNewArraySize()
    {
        assertThat(BlockUtil.calculateNewArraySize(200)).isEqualTo(300);
        assertThat(BlockUtil.calculateNewArraySize(Integer.MAX_VALUE)).isEqualTo(MAX_ARRAY_SIZE);
        try {
            BlockUtil.calculateNewArraySize(MAX_ARRAY_SIZE);
        }
        catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo(format("Cannot grow array beyond '%s'", MAX_ARRAY_SIZE));
        }
    }
}
