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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;

public interface TableInvalidationCallback
{
    default boolean isCached(Location location)
    {
        return false;
    }

    default void invalidate(Location location) {}

    default void invalidate(Partition partition) {}

    default void invalidate(Table table) {}
}
