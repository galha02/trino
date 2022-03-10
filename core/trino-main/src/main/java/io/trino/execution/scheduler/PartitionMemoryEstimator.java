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
package io.trino.execution.scheduler;

import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.spi.ErrorCode;

import static java.util.Objects.requireNonNull;

public interface PartitionMemoryEstimator
{
    MemoryRequirements getInitialMemoryRequirements(Session session, DataSize defaultMemoryLimit);

    MemoryRequirements getNextRetryMemoryRequirements(Session session, MemoryRequirements previousMemoryRequirements, ErrorCode errorCode);

    class MemoryRequirements
    {
        private final DataSize requiredMemory;
        private final boolean limitReached;

        MemoryRequirements(DataSize requiredMemory, boolean limitReached)
        {
            this.requiredMemory = requireNonNull(requiredMemory, "requiredMemory is null");
            this.limitReached = limitReached;
        }

        public DataSize getRequiredMemory()
        {
            return requiredMemory;
        }

        public boolean isLimitReached()
        {
            return limitReached;
        }
    }
}
