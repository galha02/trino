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
package io.trino.hive.formats.compression;

import io.airlift.slice.Slice;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface ValueDecompressor
        extends Closeable
{
    void decompress(Slice compressed, OutputStream uncompressed)
            throws IOException;

    void decompress(Slice compressed, Slice uncompressed)
            throws IOException;

    @Override
    default void close() {}
}
