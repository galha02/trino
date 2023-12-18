/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugins.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

// Extra Starburst Remote connector tests which require enabled writes and do not fit
// in TestStarburstRemoteDistributedQueriesWritesEnabled.
public class TestStargateWithWritesEnabledExtraTests
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithMemory(
                TpchTable.getTables(),
                Optional.empty()));
        return StargateQueryRunner.builder(remoteStarburst, "memory")
                .enableWrites()
                .build();
    }

    @Test
    public void testLargeInLongColumnName()
    {
        testLargeInLongColumnName(200);
        testLargeInLongColumnName(500);
        testLargeInLongColumnName(1000);
        testLargeInLongColumnName(5000);
    }

    private void testLargeInLongColumnName(int valuesCount)
    {
        String tableName = "test_large_column_name_" + randomNameSuffix();
        String columnName = "this_is_a_very_looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong_column_name";
        assertUpdate(format("CREATE TABLE %s (%s bigint)", tableName, columnName));

        String longValues = range(0, valuesCount)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertQuery(format("SELECT * FROM %s WHERE %s IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");

        assertQuery(format("SELECT * FROM %s WHERE %s IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
    }
}
