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

import io.airlift.units.DataSize;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int mask;
    private final int[] key;
    private final long size;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private final long hashCollisions;
    private final double expectedHashCollisions;

    public PagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        positionToHashes = new byte[addresses.size()];

        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition);
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                long hash = positionToFullHashes[position];
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(positionToHashes);
        hashCollisions = hashCollisionsLocal;
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    /**
     * return array passed as an argument for performance reasons
     */
    public long[] getAddressIndex(int[] positions, Page hashChannelsPage)
    {
        long[] hashes = new long[positions.length];
        for (int i = 0; i < positions.length; i++) {
            hashes[i] = pagesHashStrategy.hashRow(positions[i], hashChannelsPage);
        }
        return getAddressIndex(positions, hashChannelsPage, hashes);
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);

        return getAddressIndex(rightPosition, hashChannelsPage, (byte) rawHash, pos);
    }

    public long[] getAddressIndex(int[] rightPositions, Page hashChannelsPage, long[] rawHashes)
    {
        //verify(rightPositions.length == rawHashes.length, "Number of positions must match number of hashes");
        long[] result = new long[rightPositions.length];

        /*
         * Instead of looking for a match on a per-position basis, we execute every step in a "vectorized" way,
         * so that all cpu mechanisms (caching, out-of-order execution, etc.) are fully utilized.
         */
        int[] pos = new int[rightPositions.length];
        /*
         * First loop preforms a simple hashing of existing hashed values to determine
         * the hash bucket that the value belongs to.
         */
        for (int i = 0; i < rightPositions.length; i++) {
            pos[i] = getHashPosition(rawHashes[i], mask);
        }

        /*
         * This is the easiest, yet most expensive step, as it will trigger a lot of cache misses.
         * By doing this in a single loop the latencies of cpu memory prefetch for consecutive entries
         * will overlap with each other.
         */
        for (int i = 0; i < rightPositions.length; i++) {
            result[i] = key[pos[i]];
        }

        /*
         * Here the actual logic happens. If the bucket is empty or filled with the proper value
         * then the value in result array is valid and pos[i] is set to -1 to indicate that this
         * position has been taken care of. Otherwise we continue with the standard open-addressing
         * logic incrementing the position.
         */
        for (int i = 0; i < rightPositions.length; i++) {
            if (result[i] == -1) {
                pos[i] = -1;
            }
            else if (positionEqualsCurrentRowIgnoreNulls((int) result[i], (byte) rawHashes[i], rightPositions[i], hashChannelsPage)) {
                pos[i] = -1;
            }
            else {
                pos[i] = (pos[i] + 1) & mask;
            }
        }

        /*
         * Finally we look for remaining matches in a traditional way.
         */
        for (int i = 0; i < rightPositions.length; i++) {
            if (pos[i] != -1) {
                result[i] = getAddressIndex(rightPositions[i], hashChannelsPage, (byte) rawHashes[i], pos[i]);
            }
        }

        return result;
    }

    public long[] getAddressIndex(SelectedPositions rightPositions, Page hashChannelsPage, long[] rawHashes)
    {
        //verify(rightPositions.length == rawHashes.length, "Number of positions must match number of hashes");
        long[] result = new long[rightPositions.size()];

        /*
         * Instead of looking for a match on a per-position basis, we execute every step in a "vectorized" way,
         * so that all cpu mechanisms (caching, out-of-order execution, etc.) are fully utilized.
         */
        int[] pos = new int[rightPositions.size()];
        /*
         * First loop preforms a simple hashing of existing hashed values to determine
         * the hash bucket that the value belongs to.
         */
        for (int i = 0; i < rightPositions.size(); i++) {
            pos[i] = getHashPosition(rawHashes[i], mask);
        }

        /*
         * This is the easiest, yet most expensive step, as it will trigger a lot of cache misses.
         * By doing this in a single loop the latencies of cpu memory prefetch for consecutive entries
         * will overlap with each other.
         */
        for (int i = 0; i < rightPositions.size(); i++) {
            result[i] = key[pos[i]];
        }

        /*
         * Here the actual logic happens. If the bucket is empty or filled with the proper value
         * then the value in result array is valid and pos[i] is set to -1 to indicate that this
         * position has been taken care of. Otherwise we continue with the standard open-addressing
         * logic incrementing the position.
         */
        for (int i = 0; i < rightPositions.size(); i++) {
            if (result[i] == -1) {
                pos[i] = -1;
            }
            else if (positionEqualsCurrentRowIgnoreNulls((int) result[i], (byte) rawHashes[i], rightPositions.getOffset() + i, hashChannelsPage)) {
                pos[i] = -1;
            }
            else {
                pos[i] = (pos[i] + 1) & mask;
            }
        }

        /*
         * Finally we look for remaining matches in a traditional way.
         */
        for (int i = 0; i < rightPositions.size(); i++) {
            if (pos[i] != -1) {
                result[i] = getAddressIndex(rightPositions.getOffset() + i, hashChannelsPage, (byte) rawHashes[i], pos[i]);
            }
        }

        return result;
    }

    private int getAddressIndex(int rightPosition, Page hashChannelsPage, byte rawHash, int pos)
    {
        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }
}
