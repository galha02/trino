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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.Optional;

public class TrinoJsonHandler
        implements JsonHandler
{
    public TrinoJsonHandler()
    {
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema, Optional<ColumnVector> selectionVector)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public StructType deserializeStructType(String structTypeJson)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(CloseableIterator<FileStatus> fileIter, StructType physicalSchema, Optional<Predicate> predicate)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeJsonFileAtomically(String filePath, CloseableIterator<Row> data, boolean overwrite)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
