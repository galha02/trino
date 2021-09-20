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
package io.trino.operator;

import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public interface OperatorFactories
{
    OperatorFactory innerJoin(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean outputSingleMatch,
            boolean waitForBuild,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators);

    OperatorFactory probeOuterJoin(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean outputSingleMatch,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators);

    OperatorFactory lookupOuterJoin(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean waitForBuild,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators);

    OperatorFactory fullOuterJoin(
            Session session,
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory,
            boolean hasFilter,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            Optional<List<Integer>> probeOutputChannels,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockTypeOperators blockTypeOperators);

    OutputFactory partitionedOutput(
            TaskContext taskContext,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicateNullsAndAny,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            DataSize maxPagePartitioningBufferSize);
}
