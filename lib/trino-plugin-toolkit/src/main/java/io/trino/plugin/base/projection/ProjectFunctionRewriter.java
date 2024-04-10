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
package io.trino.plugin.base.projection;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Match;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.projection.ProjectFunctionRule.RewriteContext;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class ProjectFunctionRewriter<ProjectionResult, ExpressionResult>
{
    private final ConnectorExpressionRewriter<ExpressionResult> connectorExpressionRewriter;
    private final Set<ProjectFunctionRule<ProjectionResult, ExpressionResult>> rules;

    public ProjectFunctionRewriter(ConnectorExpressionRewriter<ExpressionResult> connectorExpressionRewriter, Set<ProjectFunctionRule<ProjectionResult, ExpressionResult>> rules)
    {
        this.connectorExpressionRewriter = requireNonNull(connectorExpressionRewriter, "connectorExpressionRewriter is null");
        this.rules = ImmutableSet.copyOf(requireNonNull(rules, "rules is null"));
    }

    public Map<ConnectorExpression, ProjectionResult> rewrite(ConnectorSession session, ConnectorExpression projectionExpression, Map<String, ColumnHandle> assignments)
    {
        requireNonNull(projectionExpression, "projectionExpression is null");
        requireNonNull(assignments, "assignments is null");

        RewriteContext<ExpressionResult> context = new RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }

            @Override
            public Optional<ExpressionResult> rewriteExpression(ConnectorExpression expression)
            {
                return connectorExpressionRewriter.rewrite(session, expression, assignments);
            }
        };

        Optional<ProjectionResult> rewrittenExpression = rewriteProjectionFunction(projectionExpression, context);
        if (rewrittenExpression.isPresent()) {
            return ImmutableMap.of(projectionExpression, rewrittenExpression.get());
        }

        ImmutableMap.Builder<ConnectorExpression, ProjectionResult> resultBuilder = ImmutableMap.builder();

        for (ConnectorExpression child : projectionExpression.getChildren()) {
            resultBuilder.putAll(rewrite(session, child, assignments));
        }

        return resultBuilder.build();
    }

    private Optional<ProjectionResult> rewriteProjectionFunction(ConnectorExpression projectionExpression, RewriteContext context)
    {
        for (ProjectFunctionRule<ProjectionResult, ExpressionResult> rule : rules) {
            Iterator<Match> matches = rule.getPattern().match(projectionExpression, context).iterator();
            while (matches.hasNext()) {
                Match match = matches.next();
                Optional<ProjectionResult> rewritten = rule.rewrite(projectionExpression, match.captures(), context);
                if (rewritten.isPresent()) {
                    return rewritten;
                }
            }
        }
        return Optional.empty();
    }
}
