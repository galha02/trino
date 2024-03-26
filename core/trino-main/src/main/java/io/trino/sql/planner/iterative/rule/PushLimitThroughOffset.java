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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.offset;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Math.addExact;

/**
 * Transforms:
 * <pre>
 * - Limit (row count x)
 *    - Offset (row count y)
 * </pre>
 * Into:
 * <pre>
 * - Offset (row count y)
 *    - Limit (row count x+y)
 * </pre>
 * Applies to both limit with ties and limit without ties.
 */
public class PushLimitThroughOffset
        implements Rule<LimitNode>
{
    private static final Capture<OffsetNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(
                    offset().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        OffsetNode child = captures.get(CHILD);

        long count;
        try {
            count = addExact(parent.getCount(), child.getCount());
        }
        catch (ArithmeticException e) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                child.replaceChildren(ImmutableList.of(
                        new LimitNode(
                                parent.id(),
                                child.getSource(),
                                count,
                                parent.getTiesResolvingScheme(),
                                parent.isPartial(),
                                parent.getPreSortedInputs()))));
    }
}
