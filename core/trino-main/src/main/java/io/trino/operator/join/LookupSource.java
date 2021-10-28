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
package io.trino.operator.join;

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    default void getJoinPositions(SelectedPositions positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        throw new UnsupportedOperationException();
    }

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    /**
     * In some implementations, like {@link io.trino.operator.index.IndexLookupSource}, the result of
     * getJoinPosition method relies on appendTo invocations beforehand. In that case any attempt to
     * cache join positions may results in incorrect values being returned
     *
     * @return Whether this lookup source supports caching values returned by getJoinPosition method
     */
    default boolean supportsCaching()
    {
        return false;
    }

    @Override
    void close();
}
