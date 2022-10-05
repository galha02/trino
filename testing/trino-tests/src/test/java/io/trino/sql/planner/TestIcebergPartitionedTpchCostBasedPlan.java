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

package io.trino.sql.planner;

import io.trino.tpch.TpchTable;

import java.util.stream.Stream;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPC-H queries.
 * This class is using Iceberg connector un-partitioned TPC-H tables.
 */
public class TestIcebergPartitionedTpchCostBasedPlan
        extends BaseIcebergCostBasedPlanTest
{
    @Override
    protected String getSchema()
    {
        // For documentation purposes only
        return "tpch_sf1000_orc_part";
    }

    @Override
    protected boolean isPartitioned()
    {
        return true;
    }

    @Override
    protected void doPrepareTables()
    {
        TpchTable.getTables().forEach(table -> {
            populateTableFromResource(
                    table.getTableName(),
                    "iceberg/tpch/sf1000/orc/partitioned/" + table.getTableName(),
                    "iceberg-tpch-sf1000-ORC-part/" + table.getTableName());
        });
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES.stream();
    }

    public static void main(String[] args)
    {
        new TestIcebergPartitionedTpchCostBasedPlan().generate();
    }
}
