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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public abstract class BaseFaultTolerantExecutionConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(BaseFaultTolerantExecutionConnectorTest.getExtraProperties());
    }

    protected abstract QueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception;

    public static Map<String, String> getExtraProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("retry-policy", "TASK")
                .put("query.initial-hash-partitions", "5")
                .put("fault-tolerant-execution-target-task-input-size", "10MB")
                .put("fault-tolerant-execution-target-task-split-count", "4")
                // TODO: re-enable once failure recover supported for this functionality
                .put("enable-dynamic-filtering", "false")
                .put("distributed-sort", "false")
                .build();
    }
}
