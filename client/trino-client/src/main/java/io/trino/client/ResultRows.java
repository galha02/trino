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

import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyIterator;

/**
 * Allows iterating over decoded result data in row-wise manner.
 */
public interface ResultRows
        extends Iterable<List<Object>>
{
    ResultRows NULL_ROWS = new ResultRows() {
        @Override
        public boolean isNull()
        {
            // This should be the only instance of this method returning true,
            // as this means "no rows yet" which is different from "empty rows".
            return true;
        }

        @Override
        public Iterator<List<Object>> iterator()
        {
            return emptyIterator();
        }
    };

    static ResultRows fromIterableRows(Iterable<List<Object>> values)
    {
        return values::iterator;
    }

    default boolean isNull()
    {
        return false;
    }
}
