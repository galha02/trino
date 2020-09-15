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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.operator.OperatorStats;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.prestosql.server.DynamicFilterService.DynamicFiltersStats;
import static io.prestosql.spi.predicate.Domain.none;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.prestosql.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestHiveDynamicPartitionPruning
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestHiveDynamicPartitionPruning.class);
    private static final String PARTITIONED_LINEITEM = "partitioned_lineitem";
    private static final long LINEITEM_COUNT = 60175;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(getTables())
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering-probe-blocking-timeout", "1h"))
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        // setup partitioned fact table for dynamic partition pruning
        @Language("SQL") String sql = format("CREATE TABLE %s WITH (format = 'TEXTFILE', partitioned_by=array['suppkey']) AS " +
                "SELECT orderkey, partkey, linenumber, suppkey FROM %s", PARTITIONED_LINEITEM, "tpch.tiny.lineitem");
        long start = System.nanoTime();
        long rows = (Long) getQueryRunner().execute(sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, PARTITIONED_LINEITEM, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name()) // Avoid node local DF
                .build();
    }

    @Test(timeOut = 30_000)
    public void testJoinWithEmptyBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name = 'abc'");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        assertEquals(probeStats.getInputPositions(), 0L);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 0);
    }

    @Test(timeOut = 30_000)
    public void testJoinWithSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                        "AND supplier.name = 'Supplier#000000001'");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(INTEGER, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testJoinWithNonSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is fully scanned
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.create(ValueSet.ofRanges(
                range(INTEGER, 1L, true, 100L, true)), false)
                .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testJoinLargeBuildSideNoDynamicFiltering()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem JOIN orders ON partitioned_lineitem.orderkey = orders.orderkey");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.all(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testJoinWithMultipleDynamicFiltersOnProbe()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        // supplier names Supplier#000000001 and Supplier#000000002 match suppkey 1 and 2
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM (" +
                        "SELECT supplier.suppkey FROM " +
                        "partitioned_lineitem JOIN tpch.tiny.supplier ON partitioned_lineitem.suppkey = supplier.suppkey AND supplier.name IN ('Supplier#000000001', 'Supplier#000000002')" +
                        ") t JOIN supplier ON t.suppkey = supplier.suppkey AND supplier.suppkey IN (2, 3)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 558L);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 2L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 2);

        List<DynamicFilterDomainStats> domainStats = dynamicFiltersStats.getDynamicFilterDomainStats();
        assertEquals(domainStats.size(), 2);
        domainStats.forEach(stats -> {
            assertGreaterThanOrEqual(stats.getRangeCount(), 1);
            assertEquals(stats.getDiscreteValuesCount(), 0);
        });
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithEmptyBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'abc')");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        assertEquals(probeStats.getInputPositions(), 0L);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), none(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 0);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier WHERE name = 'Supplier#000000001')");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 615);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(INTEGER, 1L).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinWithNonSelectiveBuildSide()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE suppkey IN (SELECT suppkey FROM supplier)");
        assertGreaterThan(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is fully scanned
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.create(ValueSet.ofRanges(
                range(INTEGER, 1L, true, 100L, true)), false)
                .toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    @Test(timeOut = 30_000)
    public void testSemiJoinLargeBuildSideNoDynamicFiltering()
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                getSession(),
                "SELECT * FROM partitioned_lineitem WHERE orderkey IN (SELECT orderkey FROM orders)");
        assertEquals(result.getResult().getRowCount(), LINEITEM_COUNT);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:" + PARTITIONED_LINEITEM);
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertEquals(probeStats.getInputPositions(), LINEITEM_COUNT);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), 0L);

        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.getQueryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), Domain.all(INTEGER).toString(getSession().toConnectorSession()));
        assertEquals(domainStats.getDiscreteValuesCount(), 0);
        assertEquals(domainStats.getRangeCount(), 1);
    }

    private DynamicFiltersStats getDynamicFilteringStats(QueryId queryId)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getDynamicFiltersStats();
    }
}
