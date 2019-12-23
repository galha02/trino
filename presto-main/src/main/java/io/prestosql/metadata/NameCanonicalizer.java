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
package io.prestosql.metadata;

import static java.util.Locale.ENGLISH;

public interface NameCanonicalizer
{
    NameCanonicalizer LEGACY_NAME_CANONICALIZER = (identifier, delimited) -> identifier.toLowerCase(ENGLISH);

    String canonicalize(String identifier, boolean delimited);

    default String canonicalize(NamePart namePart)
    {
        return canonicalize(namePart.getValue(), namePart.isDelimited());
    }
}
