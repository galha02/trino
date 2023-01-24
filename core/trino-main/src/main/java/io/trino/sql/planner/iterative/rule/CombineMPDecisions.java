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

import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;

// Maybe instead of a rule we can do it upon creation (When creating a MPDecision - eliminate the descendants MPDecision)
public class CombineMPDecisions
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        return plan;
    }
}
/*
      MP
    /  \
   A1  A2
   /    |
 MP     TS3      =>
|  \
F1  F2
|    |
TS1 TS2

MP
 -- A1 - F1 - TS1
 -- A1 - F2 - TS2
 -- A2 - TS3

 PushAggregationWithMP + flatten
 */

/*
// [Fragments Alternatives] Fragment {array[plans[TS+TH+MP]}
// List<MP> -> Array
// 1. PlanFragmenter
// 2. "Second Phase" optimization (MPs)

   A1
   /
 MPD          =>
|  \
F1  TS2
|
TS1

   MPD
   / \
 A1  A1       =>
 |    |
 F1  TS2
 |
 TS1

        MPD
     /   |
   A1   MPD
  /      |   \
 F1      A1   A2
 |       |    |
TS1     TS2  TS3

=>
CombineMPDecisions

         MPD
     /    |   \
   A1    A1   A2
  /       |    |
 F1      TS2  TS3
 |
TS1
 */
