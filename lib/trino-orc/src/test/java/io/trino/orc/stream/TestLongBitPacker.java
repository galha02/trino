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
package io.trino.orc.stream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static io.trino.orc.stream.TestingBitPackingUtils.unpackGeneric;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongBitPacker
{
    private static final int LENGTHS = 128;
    private static final int OFFSETS = 4;
    private static final int WIDTHS = 64;

    @Test
    public void testBasic()
            throws Exception
    {
        LongBitPacker packer = new LongBitPacker();
        for (int length = 0; length < LENGTHS; length++) {
            assertUnpacking(packer, length);
        }
    }

    private static void assertUnpacking(LongBitPacker packer, int length)
            throws IOException
    {
        for (int width = 1; width <= WIDTHS; width++) {
            for (int offset = 0; offset < OFFSETS; offset++) {
                long[] expected = new long[length + offset];
                long[] actual = new long[length + offset];
                RandomByteInputStream expectedInput = new RandomByteInputStream();
                unpackGeneric(expected, offset, length, width, expectedInput);
                RandomByteInputStream actualInput = new RandomByteInputStream();
                packer.unpack(actual, offset, length, width, actualInput);
                for (int i = offset; i < length + offset; i++) {
                    assertThat(actual[i])
                            .describedAs(format("index = %s, length = %s, width = %s, offset = %s", i, length, width, offset))
                            .isEqualTo(expected[i]);
                }
                assertThat(actualInput.getReadBytes())
                        .describedAs(format("Wrong number of bytes read for length = %s, width = %s, offset = %s", length, width, offset))
                        .isEqualTo(expectedInput.getReadBytes());
            }
        }
    }

    private static final class RandomByteInputStream
            extends InputStream
    {
        private final Random rand = new Random(0);
        private int readBytes;

        @Override
        public int read()
        {
            readBytes++;
            return rand.nextInt(256);
        }

        public int getReadBytes()
        {
            return readBytes;
        }
    }
}
