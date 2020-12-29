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
package io.trino.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import io.prestosql.client.NodeVersion;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.scheduler.FlatNetworkTopology;
import io.prestosql.execution.scheduler.NetworkLocation;
import io.prestosql.execution.scheduler.NetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.scheduler.NodeSelector;
import io.prestosql.execution.scheduler.NodeSelectorFactory;
import io.prestosql.execution.scheduler.TopologyAwareNodeSelectorConfig;
import io.prestosql.execution.scheduler.TopologyAwareNodeSelectorFactory;
import io.prestosql.execution.scheduler.UniformNodeSelectorFactory;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.util.FinalizerService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkNodeScheduler
{
    private static final int MAX_SPLITS_PER_NODE = 100;
    private static final int MAX_PENDING_SPLITS_PER_TASK_PER_NODE = 50;
    private static final int NODES = 200;
    private static final int DATA_NODES = 10_000;
    private static final int RACKS = DATA_NODES / 25;
    private static final int SPLITS = NODES * (MAX_SPLITS_PER_NODE + MAX_PENDING_SPLITS_PER_TASK_PER_NODE / 3);
    private static final int SPLIT_BATCH_SIZE = 100;
    private static final CatalogName CONNECTOR_ID = new CatalogName("test_connector_id");

    @Benchmark
    @OperationsPerInvocation(SPLITS)
    public Object benchmark(BenchmarkData data)
    {
        List<RemoteTask> remoteTasks = ImmutableList.copyOf(data.getTaskMap().values());
        Iterator<MockRemoteTaskFactory.MockRemoteTask> finishingTask = Iterators.cycle(data.getTaskMap().values());
        Iterator<Split> splits = data.getSplits().iterator();
        Set<Split> batch = new HashSet<>();
        while (splits.hasNext() || !batch.isEmpty()) {
            Multimap<InternalNode, Split> assignments = data.getNodeSelector().computeAssignments(batch, remoteTasks).getAssignments();
            for (InternalNode node : assignments.keySet()) {
                MockRemoteTaskFactory.MockRemoteTask remoteTask = data.getTaskMap().get(node);
                remoteTask.addSplits(ImmutableMultimap.<PlanNodeId, Split>builder()
                        .putAll(new PlanNodeId("sourceId"), assignments.get(node))
                        .build());
                remoteTask.startSplits(MAX_SPLITS_PER_NODE);
            }
            if (assignments.size() == batch.size()) {
                batch.clear();
            }
            else {
                batch.removeAll(assignments.values());
            }
            while (batch.size() < SPLIT_BATCH_SIZE && splits.hasNext()) {
                batch.add(splits.next());
            }
            finishingTask.next().finishSplits((int) Math.ceil(MAX_SPLITS_PER_NODE / 50.0));
        }

        return remoteTasks;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"uniform",
                "benchmark",
                "topology"})
        private String policy = "uniform";

        private FinalizerService finalizerService = new FinalizerService();
        private NodeSelector nodeSelector;
        private Map<InternalNode, MockRemoteTaskFactory.MockRemoteTask> taskMap = new HashMap<>();
        private List<Split> splits = new ArrayList<>();

        @Setup
        public void setup()
        {
            finalizerService.start();
            NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

            ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
            for (int i = 0; i < NODES; i++) {
                nodeBuilder.add(new InternalNode("node" + i, URI.create("http://" + addressForHost(i).getHostText()), NodeVersion.UNKNOWN, false));
            }
            List<InternalNode> nodes = nodeBuilder.build();
            MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(
                    newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s")),
                    newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s")));
            for (int i = 0; i < nodes.size(); i++) {
                InternalNode node = nodes.get(i);
                ImmutableList.Builder<Split> initialSplits = ImmutableList.builder();
                for (int j = 0; j < MAX_SPLITS_PER_NODE + MAX_PENDING_SPLITS_PER_TASK_PER_NODE; j++) {
                    initialSplits.add(new Split(CONNECTOR_ID, new TestSplitRemote(i), Lifespan.taskWide()));
                }
                TaskId taskId = new TaskId("test", 1, i);
                MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, initialSplits.build(), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
                nodeTaskMap.addTask(node, remoteTask);
                taskMap.put(node, remoteTask);
            }

            for (int i = 0; i < SPLITS; i++) {
                splits.add(new Split(CONNECTOR_ID, new TestSplitRemote(ThreadLocalRandom.current().nextInt(DATA_NODES)), Lifespan.taskWide()));
            }

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode(CONNECTOR_ID, nodes);
            NodeScheduler nodeScheduler = new NodeScheduler(getNodeSelectorFactory(nodeManager, nodeTaskMap));
            nodeSelector = nodeScheduler.createNodeSelector(Optional.of(CONNECTOR_ID));
        }

        @TearDown
        public void tearDown()
        {
            finalizerService.destroy();
        }

        private NodeSchedulerConfig getNodeSchedulerConfig()
        {
            return new NodeSchedulerConfig()
                    .setMaxSplitsPerNode(MAX_SPLITS_PER_NODE)
                    .setIncludeCoordinator(false)
                    .setNodeSchedulerPolicy(policy)
                    .setMaxPendingSplitsPerTask(MAX_PENDING_SPLITS_PER_TASK_PER_NODE);
        }

        private NodeSelectorFactory getNodeSelectorFactory(InMemoryNodeManager nodeManager, NodeTaskMap nodeTaskMap)
        {
            NodeSchedulerConfig nodeSchedulerConfig = getNodeSchedulerConfig();
            switch (policy) {
                case "uniform":
                    return new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap);
                case "topology":
                    return new TopologyAwareNodeSelectorFactory(new FlatNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap, new TopologyAwareNodeSelectorConfig());
                case "benchmark":
                    return new TopologyAwareNodeSelectorFactory(new BenchmarkNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap, getBenchmarkNetworkTopologyConfig());
                default:
                    throw new IllegalStateException();
            }
        }

        public Map<InternalNode, MockRemoteTaskFactory.MockRemoteTask> getTaskMap()
        {
            return taskMap;
        }

        public NodeSelector getNodeSelector()
        {
            return nodeSelector;
        }

        public List<Split> getSplits()
        {
            return splits;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkNodeScheduler.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    private static class BenchmarkNetworkTopology
            implements NetworkTopology
    {
        @Override
        public NetworkLocation locate(HostAddress address)
        {
            List<String> parts = new ArrayList<>(ImmutableList.copyOf(Splitter.on(".").split(address.getHostText())));
            Collections.reverse(parts);
            return new NetworkLocation(parts);
        }
    }

    private static TopologyAwareNodeSelectorConfig getBenchmarkNetworkTopologyConfig()
    {
        return new TopologyAwareNodeSelectorConfig()
                .setLocationSegmentNames(ImmutableList.of("rack", "machine"));
    }

    private static class TestSplitRemote
            implements ConnectorSplit
    {
        private final List<HostAddress> hosts;

        public TestSplitRemote(int dataHost)
        {
            hosts = ImmutableList.of(addressForHost(dataHost));
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return hosts;
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    private static HostAddress addressForHost(int host)
    {
        int rack = Integer.hashCode(host) % RACKS;
        return HostAddress.fromParts("host" + host + ".rack" + rack, 1);
    }
}
