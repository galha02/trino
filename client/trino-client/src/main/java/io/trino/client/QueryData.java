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
package io.trino.client;

import jakarta.annotation.Nullable;

import java.util.List;

/**
 * Used for representing both raw JSON values and spooled metadata.
 */
public interface QueryData
{
    @Deprecated
    @Nullable
    default Iterable<List<Object>> getData()
    {
        throw new UnsupportedOperationException("getData() is deprecated for removal, use StatementClient.currentRows() instead");
    }

    boolean isNull();
}
