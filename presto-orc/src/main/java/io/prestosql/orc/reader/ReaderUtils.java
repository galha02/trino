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
package io.prestosql.orc.reader;

import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.StreamDescriptor;
import io.prestosql.spi.type.Type;

import java.util.function.Predicate;

final class ReaderUtils
{
    private ReaderUtils() {}

    public static void verifyStreamType(StreamDescriptor streamDescriptor, Type actual, Predicate<Type> validTypes)
            throws OrcCorruptionException
    {
        if (validTypes.test(actual)) {
            return;
        }

        throw new OrcCorruptionException(
                streamDescriptor.getOrcDataSourceId(),
                "Can not read SQL type %s from ORC stream %s of type %s",
                actual,
                streamDescriptor.getStreamName(),
                streamDescriptor.getStreamType());
    }
}
