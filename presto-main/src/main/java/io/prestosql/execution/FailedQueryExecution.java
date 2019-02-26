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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.planner.Plan;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FailedQueryExecution
        implements QueryExecution
{
    private final Session session;
    private final Executor executor;
    private final QueryInfo queryInfo;

    public FailedQueryExecution(Session session, String query, URI self, Optional<ResourceGroupId> resourceGroup, Executor executor, Throwable cause)
    {
        requireNonNull(cause, "cause is null");
        this.session = requireNonNull(session, "session is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.queryInfo = immediateFailureQueryInfo(session, query, self, resourceGroup, cause);
    }

    @Override
    public QueryId getQueryId()
    {
        return queryInfo.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    @Override
    public QueryState getState()
    {
        return queryInfo.getState();
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return new VersionedMemoryPoolId(GENERAL_POOL, 0);
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        // no-op
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, NANOSECONDS);
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public DateTime getCreateTime()
    {
        return queryInfo.getQueryStats().getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return Optional.ofNullable(queryInfo.getQueryStats().getExecutionStartTime());
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return queryInfo.getQueryStats().getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return Optional.ofNullable(queryInfo.getQueryStats().getEndTime());
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return new BasicQueryInfo(getQueryInfo());
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        // no-op
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return immediateFuture(queryInfo.getState());
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        executor.execute(() -> stateChangeListener.stateChanged(FAILED));
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        executor.execute(() -> stateChangeListener.stateChanged(queryInfo));
    }

    @Override
    public void fail(Throwable cause)
    {
        // no-op
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void cancelQuery()
    {
        // no-op
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        // no-op
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    private static QueryInfo immediateFailureQueryInfo(Session session, String query, URI self, Optional<ResourceGroupId> resourceGroupId, Throwable throwable)
    {
        ExecutionFailureInfo failureCause = toFailure(throwable);
        QueryInfo queryInfo = new QueryInfo(
                session.getQueryId(),
                session.toSessionRepresentation(),
                FAILED,
                GENERAL_POOL,
                false,
                self,
                ImmutableList.of(),
                query,
                immediateFailureQueryStats(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.empty(),
                failureCause,
                failureCause.getErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                true,
                resourceGroupId);

        return queryInfo;
    }

    private static QueryStats immediateFailureQueryStats()
    {
        DateTime now = DateTime.now();
        return new QueryStats(
                now,
                now,
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                false,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                0,
                new DataSize(0, BYTE),
                ImmutableList.of(),
                ImmutableList.of());
    }
}
