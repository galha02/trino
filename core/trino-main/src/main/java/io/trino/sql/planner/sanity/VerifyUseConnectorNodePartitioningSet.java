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
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;

public final class VerifyUseConnectorNodePartitioningSet
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        searchFrom(plan)
                .where(TableScanNode.class::isInstance)
                .<TableScanNode>findAll()
                .stream()
                .filter(scan -> scan.getUseConnectorNodePartitioning().isEmpty())
                .forEach(scan -> {
                    throw new IllegalStateException(format("TableScanNode (%s) doesn't have useConnectorNodePartitioning set", scan));
                });
    }
}
