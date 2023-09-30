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

import static org.assertj.core.api.Assertions.assertThat;

public class TestIntArrayList
{
    private static final int N_ELEMENTS = 1000;

    @Test
    public void testAddsElements()
    {
        IntArrayList list = new IntArrayList(0);

        for (int i = 0; i < N_ELEMENTS; ++i) {
            list.add(i);
        }

        assertThat(list.size()).isEqualTo(N_ELEMENTS);

        int[] elements = list.elements();
        for (int i = 0; i < N_ELEMENTS; ++i) {
            assertThat(elements[i]).isEqualTo(i);
        }
    }
}
