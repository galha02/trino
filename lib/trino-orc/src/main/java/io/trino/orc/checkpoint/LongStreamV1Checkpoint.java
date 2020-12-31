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
package io.trino.orc.checkpoint;

import com.google.common.collect.ImmutableList;
import io.trino.orc.checkpoint.Checkpoints.ColumnPositionsList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.createInputStreamPositionList;
import static io.trino.orc.checkpoint.InputStreamCheckpoint.inputStreamCheckpointToString;

public class LongStreamV1Checkpoint
        implements LongStreamCheckpoint
{
    private final int offset;
    private final long inputStreamCheckpoint;

    public LongStreamV1Checkpoint(int offset, long inputStreamCheckpoint)
    {
        this.offset = offset;
        this.inputStreamCheckpoint = inputStreamCheckpoint;
    }

    public LongStreamV1Checkpoint(boolean compressed, ColumnPositionsList positionsList)
    {
        inputStreamCheckpoint = createInputStreamCheckpoint(compressed, positionsList);
        offset = positionsList.nextPosition();
    }

    public int getOffset()
    {
        return offset;
    }

    public long getInputStreamCheckpoint()
    {
        return inputStreamCheckpoint;
    }

    @Override
    public List<Integer> toPositionList(boolean compressed)
    {
        return ImmutableList.<Integer>builder()
                .addAll(createInputStreamPositionList(compressed, inputStreamCheckpoint))
                .add(offset)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("inputStreamCheckpoint", inputStreamCheckpointToString(inputStreamCheckpoint))
                .toString();
    }
}
