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
package io.trino.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.Min;

public class JdbcMetadataConfig
{
    private boolean allowDropTable;
    /*
     * Join pushdown is disabled by default as this is the safer option.
     * Pushing down a join which substantially increases the row count vs
     * sizes of left and right table separately, may incur huge cost both
     * in terms of performance and money due to an increased network traffic.
     */
    private boolean joinPushdownEnabled;
    private boolean aggregationPushdownEnabled = true;

    private boolean topNPushdownEnabled = true;

    // Pushed domains are transformed into SQL IN lists
    // (or sequence of range predicates) in JDBC connectors.
    // Too large IN lists cause significant performance regression.
    // Use 32 as compaction threshold as it provides reasonable balance
    // between performance and pushdown capabilities
    private int domainCompactionThreshold = 32;

    // The limit of the values per IN operator depends on the database. 
    // E.g. Oracle allows only up to 1,000 IN list values in a SQL statement.
    // A value of 0 means no limit
    private int inOperatorLimit = 0;

    public boolean isAllowDropTable()
    {
        return allowDropTable;
    }

    @Config("allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public JdbcMetadataConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public boolean isJoinPushdownEnabled()
    {
        return joinPushdownEnabled;
    }

    @LegacyConfig("experimental.join-pushdown.enabled")
    @Config("join-pushdown.enabled")
    @ConfigDescription("Enable join pushdown")
    public JdbcMetadataConfig setJoinPushdownEnabled(boolean joinPushdownEnabled)
    {
        this.joinPushdownEnabled = joinPushdownEnabled;
        return this;
    }

    public boolean isAggregationPushdownEnabled()
    {
        return aggregationPushdownEnabled;
    }

    @Config("aggregation-pushdown.enabled")
    @LegacyConfig("allow-aggregation-pushdown")
    @ConfigDescription("Enable aggregation pushdown")
    public JdbcMetadataConfig setAggregationPushdownEnabled(boolean aggregationPushdownEnabled)
    {
        this.aggregationPushdownEnabled = aggregationPushdownEnabled;
        return this;
    }

    @Config("topn-pushdown.enabled")
    @ConfigDescription("Enable TopN pushdown")
    public JdbcMetadataConfig setTopNPushdownEnabled(boolean enabled)
    {
        this.topNPushdownEnabled = enabled;
        return this;
    }

    public Boolean isTopNPushdownEnabled()
    {
        return this.topNPushdownEnabled;
    }

    @Min(1)
    public int getDomainCompactionThreshold()
    {
        return domainCompactionThreshold;
    }

    @Config("domain-compaction-threshold")
    @ConfigDescription("Maximum ranges to allow in a tuple domain without compacting it")
    public JdbcMetadataConfig setDomainCompactionThreshold(int domainCompactionThreshold)
    {
        this.domainCompactionThreshold = domainCompactionThreshold;
        return this;
    }

    @Min(0)
    public int getInOperatorLimit()
    {
        return inOperatorLimit;
    }    

    @Config("in-operator-limit")
    @ConfigDescription("Maximum number of values per IN operator. A value of 0 means no limit")
    public JdbcMetadataConfig setInOperatorLimit(int inOperatorLimit)
    {
        this.inOperatorLimit = inOperatorLimit;
        return this;
    }    
}
