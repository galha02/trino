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
package io.trino.sql.planner.sanity;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Optional;

import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public class TableExecuteStructureValidator
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, TypeOperators typeOperators, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        Optional<TableWriterNode.WriterTarget> writerTarget = searchFrom(planNode)
                .where(node -> node instanceof TableWriterNode)
                .findFirst()
                .map(node -> ((TableWriterNode) node).getTarget());

        if (writerTarget.isEmpty() || !(writerTarget.get() instanceof TableWriterNode.TableExecuteTarget)) {
            // we are good; not a TableExecute plan
            return;
        }

        searchFrom(planNode)
                .findAll()
                .forEach(node -> {
                    if (!isAllowedNode(node)) {
                        throw new IllegalStateException("Unexpected " + node.getClass().getSimpleName() + " found in plan; probably connector was not able to handle provided WHERE expression");
                    }
                });
    }

    private boolean isAllowedNode(PlanNode node)
    {
        return node instanceof TableScanNode
                || node instanceof ValuesNode
                || node instanceof ProjectNode
                || node instanceof TableWriterNode
                || node instanceof OutputNode
                || node instanceof ExchangeNode
                || node instanceof TableFinishNode;
    }
}
