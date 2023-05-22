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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

public interface FlatDefinitionLevelDecoder
{
    void init(Slice input);

    /**
     * Populate 'values' with true for nulls and return the number of non-nulls encountered.
     * 'values' array is assumed to be empty at the start of reading a batch, i.e. contain only false values.
     */
    int readNext(boolean[] values, int offset, int length);

    /**
     * Skip 'length' values and return the number of non-nulls encountered
     */
    int skip(int length);

    interface DefinitionLevelDecoderProvider
    {
        FlatDefinitionLevelDecoder create(int maxDefinitionLevel);
    }

    static FlatDefinitionLevelDecoder getFlatDefinitionLevelDecoder(int maxDefinitionLevel)
    {
        checkArgument(maxDefinitionLevel >= 0 && maxDefinitionLevel <= 1, "Invalid max definition level: " + maxDefinitionLevel);
        if (maxDefinitionLevel == 0) {
            return new ZeroDefinitionLevelDecoder();
        }
        return new NullsDecoder();
    }
}
