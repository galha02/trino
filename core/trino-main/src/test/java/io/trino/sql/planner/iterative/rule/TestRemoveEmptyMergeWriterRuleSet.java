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

import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableFinishNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;

public class TestRemoveEmptyMergeWriterRuleSet
        extends BaseRuleTest
{
    private CatalogHandle catalogSupportingScaledWriters;
    private SchemaTableName schemaTableName;

    @BeforeClass
    public void setup()
    {
        catalogSupportingScaledWriters = createTestCatalogHandle("catalog");
        schemaTableName = new SchemaTableName("schema", "table");
    }

    @DataProvider
    public static Object[][] testRemoveEmptyMergeRewriteParams()
    {
        return new Object[][] {
                {RemoveEmptyMergeWriterRuleSet.removeEmptyMergeWriterRule(), false},
                {RemoveEmptyMergeWriterRuleSet.removeEmptyMergeWriterWithExchangeRule(), true}
        };
    }

    @Test(dataProvider = "testRemoveEmptyMergeRewriteParams")
    public void testRemoveEmptyMergeRewriteFires(Rule<TableFinishNode> rule, boolean withExchange)
    {
        tester().assertThat(rule)
                .on(p -> {
                    Symbol mergeRow = p.symbol("merge_row");
                    Symbol rowId = p.symbol("row_id");
                    Symbol rowCount = p.symbol("row_count");

                    PlanNode merge = p.merge(
                            schemaTableName,
                            p.exchange(e -> e
                                    .addSource(
                                            p.project(
                                                    Assignments.builder()
                                                            .putIdentity(mergeRow)
                                                            .putIdentity(rowId)
                                                            .putIdentity(rowCount)
                                                            .build(),
                                                    p.values(mergeRow, rowId, rowCount)))
                                    .addInputsSet(mergeRow, rowId, rowCount)
                                    .partitioningScheme(
                                            new PartitioningScheme(
                                                    Partitioning.create(SINGLE_DISTRIBUTION, List.of()),
                                                    List.of(mergeRow, rowId, rowCount)))),
                            mergeRow,
                            rowId,
                            List.of(rowCount));
                    return p.tableFinish(
                            withExchange ? withExchange(p, merge, rowCount) : merge,
                            p.createTarget(catalogSupportingScaledWriters, schemaTableName, true, true),
                            rowCount);
                })
                .matches(values("A"));
    }

    private ExchangeNode withExchange(PlanBuilder planBuilder, PlanNode source, Symbol symbol)
    {
        return planBuilder.exchange(e -> e
                .addSource(source)
                .addInputsSet(symbol)
                .partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, List.of()), List.of(symbol))));
    }
}
