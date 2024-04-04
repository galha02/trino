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
package io.trino.hive.formats.avro.model;

import io.airlift.slice.Slice;
import org.apache.avro.Schema;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record EnumReadAction(Schema readSchema, Schema writeSchema, List<Slice> symbolIndex)
        implements AvroReadAction
{
    public EnumReadAction
    {
        requireNonNull(readSchema, "readSchema is null");
        requireNonNull(writeSchema, "writeSchema is null");
        requireNonNull(symbolIndex, "symbolIndex is null");
    }

    // Decoder::readInt returns the index of the symbol to use
    public List<Slice> getSymbolIndex()
    {
        return symbolIndex;
    }
}
