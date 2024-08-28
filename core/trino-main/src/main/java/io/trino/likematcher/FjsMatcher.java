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
package io.trino.likematcher;

import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FjsMatcher
        implements Matcher
{
    private final List<Pattern> pattern;
    private final int start;
    private final int end;
    private final boolean exact;

    private volatile Fjs matcher;

    public FjsMatcher(List<Pattern> pattern, int start, int end, boolean exact)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.start = start;
        this.end = end;
        this.exact = exact;
    }

    @Override
    public boolean match(Slice input, int offset, int length)
    {
        Fjs matcher = this.matcher;
        if (matcher == null) {
            matcher = new Fjs(pattern, start, end, exact);
            this.matcher = matcher;
        }

        return matcher.match(input, offset, length);
    }

    private static class Fjs
    {
        private final boolean exact;
        private final List<Slice> patterns = new ArrayList<>();
        private final List<int[]> bmsShifts = new ArrayList<>();
        private final List<int[]> kmpShifts = new ArrayList<>();

        public Fjs(List<Pattern> pattern, int start, int end, boolean exact)
        {
            this.exact = exact;

            for (int i = start; i <= end; i++) {
                Pattern element = pattern.get(i);

                switch (element) {
                    case Pattern.Literal(Slice bytes) -> {
                        checkArgument(i == 0 || !(pattern.get(i - 1) instanceof Pattern.Literal), "Multiple consecutive literals found");
                        patterns.add(bytes);
                        bmsShifts.add(computeBmsShifts(bytes));
                        kmpShifts.add(computeKmpShifts(bytes));
                    }
                    case Pattern.Any _ -> throw new IllegalArgumentException("'any' pattern not supported");
                    case null, default -> {}
                }
            }
        }

        private static int[] computeKmpShifts(Slice pattern)
        {
            int[] result = new int[pattern.length() + 1];
            result[0] = -1;

            int j = -1;
            for (int i = 1; i < result.length; i++) {
                while (j >= 0 && pattern.getByte(i - 1) != pattern.getByte(j)) {
                    j = result[j];
                }
                j++;
                result[i] = j;
            }

            return result;
        }

        private static int[] computeBmsShifts(Slice pattern)
        {
            int[] result = new int[256];

            for (int i = 0; i < pattern.length(); i++) {
                result[pattern.getByte(i) & 0xFF] = i + 1;
            }

            return result;
        }

        private static int find(Slice input, final int offset, final int length, Slice pattern, int[] bmsShifts, int[] kmpShifts)
        {
            int patternLength = pattern.length();
            if (patternLength > length || patternLength == 0) {
                return -1;
            }

            final int inputLimit = offset + length;
            byte lastByte = pattern.getByte(patternLength - 1);

            int i = offset;
            while (true) {
                // Attempt to match the last position of the pattern
                // As long as it doesn't match, skip ahead based on the Boyer-Moore-Sunday heuristic
                int matchEnd = i + pattern.length() - 1;
                while (matchEnd < inputLimit - 1 && input.getByte(matchEnd) != lastByte) {
                    int shift = patternLength + 1 - bmsShifts[input.getByte(matchEnd + 1) & 0xFF];
                    matchEnd += shift;
                }

                if (matchEnd == inputLimit - 1 && match(input, inputLimit - patternLength, pattern)) {
                    return inputLimit - patternLength;
                }
                else if (matchEnd >= inputLimit - 1) {
                    return -1;
                }

                // At this point, we know the last position of the pattern matches with some
                // position in the input text given by "matchEnd"
                // Use KMP to match the first length-1 characters of the pattern

                i = matchEnd - (patternLength - 1);

                int j = findLongestMatch(input, i, pattern, 0, patternLength - 1);

                if (j == pattern.length() - 1) {
                    return i;
                }

                i += j;
                j = kmpShifts[j];

                // Continue to match the whole pattern using KMP
                while (j >= 0) {
                    int size = findLongestMatch(input, i, pattern, j, Math.min(inputLimit - i, patternLength - j));
                    i += size;
                    j += size;

                    if (j == patternLength) {
                        return i - j;
                    }

                    j = kmpShifts[j];
                }

                i++;
            }
        }

        private static int findLongestMatch(Slice input, int inputOffset, Slice pattern, int patternOffset, int length)
        {
            for (int i = 0; i < length; i++) {
                if (input.getByte(inputOffset + i) != pattern.getByte(patternOffset + i)) {
                    return i;
                }
            }
            return length;
        }

        private static boolean match(Slice input, int offset, Slice pattern)
        {
            for (int i = 0; i < pattern.length(); i++) {
                if (input.getByte(offset + i) != pattern.getByte(i)) {
                    return false;
                }
            }
            return true;
        }

        public boolean match(Slice input, int offset, int length)
        {
            int start = offset;
            int remaining = length;

            for (int i = 0; i < patterns.size(); i++) {
                if (remaining == 0) {
                    return false;
                }

                Slice term = patterns.get(i);

                int position = find(input, start, remaining, term, bmsShifts.get(i), kmpShifts.get(i));
                if (position == -1) {
                    return false;
                }

                position += term.length();
                remaining -= position - start;
                start = position;
            }

            return !exact || remaining == 0;
        }
    }
}
