/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.prestosql.testing.QueryRunner;

import java.util.Set;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.connectionProperties;
import static io.prestosql.tpch.TpchTable.ORDERS;

public class TestOracleDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withTables(Set.of(ORDERS))
                .withConnectorProperties(connectionProperties())
                .build();
    }

    @Override
    protected boolean isAssertNumberOfSplits()
    {
        return false; // Dynamic filters do not allow Oracle to eliminate splits
    }
}
