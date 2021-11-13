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
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Row;

import java.util.Set;

import static io.trino.sql.planner.plan.Patterns.delete;
import static io.trino.sql.planner.plan.Patterns.emptyValues;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableFinish;
import static java.util.Objects.requireNonNull;

/**
 * If the predicate for a delete is optimized to false, the target table scan
 * of the delete will be replaced with an empty values node. This type of
 * plan cannot be executed and is meaningless anyway, so we replace the
 * entire thing with a values node.
 * <p>
 * Transforms
 * <pre>
 *  - TableFinish
 *    - Exchange (optional)
 *      - Delete
 *        - empty Values
 * </pre>
 * into
 * <pre>
 *  - Values (0)
 * </pre>
 */
// TODO split into multiple rules (https://github.com/prestodb/presto/issues/7292)
public final class RemoveEmptyDeleteRuleSet
{
    private RemoveEmptyDeleteRuleSet() {}

    public static Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                remoteEmptyDeleteRule(),
                removeEmptyDeleteWithExchangeRule());
    }

    static Rule<TableFinishNode> remoteEmptyDeleteRule()
    {
        return new RemoveEmptyDelete(tableFinish()
                .with(source().matching(delete()
                        .with(source().matching(emptyValues())))));
    }

    static Rule<TableFinishNode> removeEmptyDeleteWithExchangeRule()
    {
        return new RemoveEmptyDelete(tableFinish()
                .with(source().matching(exchange()
                        .with(source().matching(delete()
                                .with(source().matching(emptyValues())))))));
    }

    private static final class RemoveEmptyDelete
            implements Rule<TableFinishNode>
    {
        private final Pattern<TableFinishNode> pattern;

        private RemoveEmptyDelete(Pattern<TableFinishNode> pattern)
        {
            this.pattern = requireNonNull(pattern, "pattern is null");
        }

        @Override
        public Pattern<TableFinishNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(TableFinishNode node, Captures captures, Context context)
        {
            return Result.ofPlanNode(
                    new ValuesNode(
                            node.getId(),
                            node.getOutputSymbols(),
                            ImmutableList.of(new Row(ImmutableList.of(new GenericLiteral("BIGINT", "0"))))));
        }
    }
}
