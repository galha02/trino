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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.memory.MemoryInfo;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.execution.scheduler.FallbackToFullNodePartitionMemoryEstimator.FULL_NODE_MEMORY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

// uses mutable state
@Test(singleThreaded = true)
public class TestFullNodeCapableNodeAllocator
{
    private static final Session Q1_SESSION = testSessionBuilder().setQueryId(QueryId.valueOf("q1")).build();
    private static final Session Q2_SESSION = testSessionBuilder().setQueryId(QueryId.valueOf("q2")).build();

    private static final HostAddress NODE_1_ADDRESS = HostAddress.fromParts("127.0.0.1", 8080);
    private static final HostAddress NODE_2_ADDRESS = HostAddress.fromParts("127.0.0.1", 8081);
    private static final HostAddress NODE_3_ADDRESS = HostAddress.fromParts("127.0.0.1", 8082);
    private static final HostAddress NODE_4_ADDRESS = HostAddress.fromParts("127.0.0.1", 8083);

    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("local://" + NODE_1_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("local://" + NODE_2_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("local://" + NODE_3_ADDRESS), NodeVersion.UNKNOWN, false);
    private static final InternalNode NODE_4 = new InternalNode("node-4", URI.create("local://" + NODE_4_ADDRESS), NodeVersion.UNKNOWN, false);

    private static final CatalogName CATALOG_1 = new CatalogName("catalog1");
    private static final CatalogName CATALOG_2 = new CatalogName("catalog2");
    private static final List<CatalogName> ALL_CATALOGS = ImmutableList.of(CATALOG_1, CATALOG_2);

    private static final NodeRequirements NO_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(), DataSize.of(32, GIGABYTE));
    private static final NodeRequirements SHARED_NODE_CATALOG_1_REQUIREMENTS = new NodeRequirements(Optional.of(CATALOG_1), Set.of(), DataSize.of(32, GIGABYTE));
    private static final NodeRequirements FULL_NODE_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(), FULL_NODE_MEMORY);
    private static final NodeRequirements FULL_NODE_2_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(NODE_2_ADDRESS), FULL_NODE_MEMORY);
    private static final NodeRequirements FULL_NODE_3_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(NODE_3_ADDRESS), FULL_NODE_MEMORY);
    private static final NodeRequirements FULL_NODE_4_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(NODE_4_ADDRESS), FULL_NODE_MEMORY);
    private static final NodeRequirements FULL_NODE_CATALOG_1_REQUIREMENTS = new NodeRequirements(Optional.of(CATALOG_1), Set.of(), FULL_NODE_MEMORY);
    private static final NodeRequirements FULL_NODE_CATALOG_2_REQUIREMENTS = new NodeRequirements(Optional.of(CATALOG_2), Set.of(), FULL_NODE_MEMORY);
    // not using FULL_NODE_MEMORY marker but with memory requirements exceeding any node in cluster
    private static final NodeRequirements EFFECTIVELY_FULL_NODE_REQUIREMENTS = new NodeRequirements(Optional.empty(), Set.of(), DataSize.of(65, GIGABYTE));

    // none of the tests should require periodic execution of routine which processes pending acquisitions
    private static final long TEST_TIMEOUT = FullNodeCapableNodeAllocatorService.PROCESS_PENDING_ACQUIRES_DELAY_SECONDS * 1000 / 2;

    private FullNodeCapableNodeAllocatorService nodeAllocatorService;

    private void setupNodeAllocatorService(InMemoryNodeManager nodeManager, int maxFullNodesPerQuery)
    {
        shutdownNodeAllocatorService(); // just in case

        MemoryInfo memoryInfo = new MemoryInfo(4, new MemoryPoolInfo(DataSize.of(64, GIGABYTE).toBytes(), 0, 0, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

        Map<String, Optional<MemoryInfo>> workerMemoryInfos = ImmutableMap.of(
                NODE_1.getNodeIdentifier(), Optional.of(memoryInfo),
                NODE_2.getNodeIdentifier(), Optional.of(memoryInfo),
                NODE_3.getNodeIdentifier(), Optional.of(memoryInfo),
                NODE_4.getNodeIdentifier(), Optional.of(memoryInfo));

        nodeAllocatorService = new FullNodeCapableNodeAllocatorService(
                nodeManager,
                () -> workerMemoryInfos,
                maxFullNodesPerQuery,
                1.0,
                false);
        nodeAllocatorService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdownNodeAllocatorService()
    {
        if (nodeAllocatorService != null) {
            nodeAllocatorService.stop();
        }
        nodeAllocatorService = null;
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateSharedSimple()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // first two allocation should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire2);
            // and different nodes should be assigned for each
            assertThat(Set.of(acquire1.getNode().get(), acquire2.getNode().get())).containsExactlyInAnyOrder(NODE_1, NODE_2);

            // same for subsequent two allocation (each task requires 32GB and we have 2 nodes with 64GB each)
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire3);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire4);
            assertThat(Set.of(acquire3.getNode().get(), acquire4.getNode().get())).containsExactlyInAnyOrder(NODE_1, NODE_2);

            // 5th allocation should block
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertNotAcquired(acquire5);

            // release acquire2 which uses
            acquire2.release();
            assertEventually(() -> {
                // we need to wait as pending acquires are processed asynchronously
                assertAcquired(acquire5);
                assertEquals(acquire5.getNode().get(), acquire2.getNode().get());
            });

            // try to acquire one more node (should block)
            NodeAllocator.NodeLease acquire6 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertNotAcquired(acquire6);

            // add new node
            addNode(nodeManager, NODE_3);
            // TODO: make FullNodeCapableNodeAllocatorService react on new node added automatically
            nodeAllocatorService.wakeupProcessPendingAcquires();

            // new node should be assigned
            assertEventually(() -> {
                assertAcquired(acquire6);
                assertEquals(acquire6.getNode().get(), NODE_3);
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateSharedReleaseBeforeAcquired()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // first two allocation should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire2, NODE_1);

            // another two should block
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertNotAcquired(acquire3);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertNotAcquired(acquire4);

            // releasing a blocked one should not unblock anything
            acquire3.release();
            assertNotAcquired(acquire4);

            // releasing an acquired one should unblock one which is still blocked
            acquire2.release();
            assertEventually(() -> assertAcquired(acquire4, NODE_1));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testNoSharedNodeAvailable()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(nodesMapBuilder()
                .put(NODE_1, ImmutableList.of(CATALOG_2))
                .buildOrThrow());
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // request a node with specific catalog (not present)

            assertThatThrownBy(() -> nodeAllocator.acquire(SHARED_NODE_CATALOG_1_REQUIREMENTS.withMemory(DataSize.of(64, GIGABYTE))))
                    .hasMessage("No nodes available to run query");

            // add node with specific catalog
            addNode(nodeManager, NODE_2, CATALOG_1);

            // we should be able to acquire the node now
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(SHARED_NODE_CATALOG_1_REQUIREMENTS.withMemory(DataSize.of(64, GIGABYTE)));
            assertAcquired(acquire1, NODE_2);

            // acquiring one more should block (only one acquire fits a node as we request 64GB)
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(SHARED_NODE_CATALOG_1_REQUIREMENTS.withMemory(DataSize.of(64, GIGABYTE)));
            assertNotAcquired(acquire2);

            // remove node with catalog
            nodeManager.removeNode(NODE_2);
            // TODO: make FullNodeCapableNodeAllocatorService react on node removed automatically
            nodeAllocatorService.wakeupProcessPendingAcquires();

            // pending acquire2 should be completed now but with an exception
            assertEventually(() -> {
                assertFalse(acquire2.getNode().isCancelled());
                assertTrue(acquire2.getNode().isDone());
                assertThatThrownBy(() -> getFutureValue(acquire2.getNode()))
                        .hasMessage("No nodes available to run query");
            });
        }
    }

    private InMemoryNodeManager testingNodeManager(Map<InternalNode, List<CatalogName>> nodeMap)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        for (Map.Entry<InternalNode, List<CatalogName>> entry : nodeMap.entrySet()) {
            InternalNode node = entry.getKey();
            List<CatalogName> catalogs = entry.getValue();
            for (CatalogName catalog : catalogs) {
                nodeManager.addNode(catalog, node);
            }
        }
        return nodeManager;
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testRemoveAcquiredSharedNode()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);

            // remove acquired node
            nodeManager.removeNode(NODE_1);

            // we should still be able to release lease for removed node
            acquire1.release();
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullSimple()
            throws Exception
    {
        testAllocateFullSimple(FULL_NODE_REQUIREMENTS);
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testEffectivelyFullNodeSimple()
            throws Exception
    {
        testAllocateFullSimple(EFFECTIVELY_FULL_NODE_REQUIREMENTS);
    }

    private void testAllocateFullSimple(NodeRequirements fullNodeRequirements)
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2));
        setupNodeAllocatorService(nodeManager, 3);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // allocate 2 full nodes should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(fullNodeRequirements);
            assertAcquired(acquire1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(fullNodeRequirements);
            assertAcquired(acquire2);

            // trying to allocate third full node should block
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(fullNodeRequirements);
            assertNotAcquired(acquire3);

            // third acquisition should unblock if one of old ones is released
            acquire1.release();
            assertEventually(() -> {
                assertAcquired(acquire3);
                assertEquals(acquire3.getNode().get(), acquire1.getNode().get());
            });

            // both nodes are used exclusively so we should no be able to acquire shared node
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertNotAcquired(acquire4);

            // shared acquisition should unblock if one of full ones is released
            acquire2.release();
            assertEventually(() -> {
                assertAcquired(acquire4);
                assertEquals(acquire4.getNode().get(), acquire2.getNode().get());
            });

            // shared acquisition should block full acquisition
            NodeAllocator.NodeLease acquire5 = nodeAllocator.acquire(fullNodeRequirements);
            assertNotAcquired(acquire5);

            // and when shared acquisition is gone full node should be acquired
            acquire4.release();
            assertEventually(() -> {
                assertAcquired(acquire5);
                assertEquals(acquire5.getNode().get(), acquire4.getNode().get());
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullReleaseBeforeAcquired()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // first allocation should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);

            // another two should block
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire2);
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire3);

            // releasing a blocked one should not unblock anything
            acquire2.release();
            assertNotAcquired(acquire3);

            // releasing one acquired one should unblock one which is still blocked
            acquire1.release();
            assertEventually(() -> assertAcquired(acquire3, NODE_1));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullWithQueryLimit()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2, NODE_3));
        setupNodeAllocatorService(nodeManager, 2);

        try (NodeAllocator q1NodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION);
                NodeAllocator q2NodeAllocator = nodeAllocatorService.getNodeAllocator(Q2_SESSION)) {
            // allocate 2 full nodes for Q1 should not block
            NodeAllocator.NodeLease q1Acquire1 = q1NodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(q1Acquire1);
            NodeAllocator.NodeLease q1Acquire2 = q1NodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(q1Acquire2);

            // third allocation for Q1 should block even though we have 3 nodes available
            NodeAllocator.NodeLease q1Acquire3 = q1NodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(q1Acquire3);

            // we should still be able to acquire full node for another query
            NodeAllocator.NodeLease q2Acquire1 = q2NodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(q2Acquire1);

            // when we release one of the nodes for Q1 pending q1Acquire3 should unblock
            q1Acquire1.release();
            assertEventually(() -> {
                assertAcquired(q1Acquire3);
                assertEquals(q1Acquire3.getNode().get(), q1Acquire1.getNode().get());
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullOpportunistic()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2));
        setupNodeAllocatorService(nodeManager, 2);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // allocate both nodes as shared
            NodeAllocator.NodeLease shared1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(shared1);
            NodeAllocator.NodeLease shared2 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(shared2);

            // try to allocate 2 full nodes - will block as both nodes in cluster are used
            NodeAllocator.NodeLease full1 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(full1);
            NodeAllocator.NodeLease full2 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(full2);

            // add new node to the cluster
            addNode(nodeManager, NODE_3);
            // TODO: make FullNodeCapableNodeAllocatorService react on new node added automatically
            nodeAllocatorService.wakeupProcessPendingAcquires();

            // one of the full1/full2 should be not blocked now
            assertEventually(() -> assertTrue(full1.getNode().isDone() ^ full2.getNode().isDone(), "exactly one of full1/full2 should be unblocked"));
            NodeAllocator.NodeLease fullBlocked = full1.getNode().isDone() ? full2 : full1;
            NodeAllocator.NodeLease fullNotBlocked = full1.getNode().isDone() ? full1 : full2;

            // and when unblocked one releases node the other should grab it
            fullNotBlocked.release();
            nodeAllocatorService.wakeupProcessPendingAcquires();
            assertEventually(() -> assertAcquired(fullBlocked));
        }
    }

    @Test(timeOut = TEST_TIMEOUT * 1000)
    public void testAllocateFullWithAddressRequirements()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2, NODE_3, NODE_4));

        setupNodeAllocatorService(nodeManager, 2);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // note testing with NODE_1 as it has two incarnations when InMemoryNodeManager is in use (coordinator and non-coordinator, sharing address)
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_2_REQUIREMENTS);
            assertAcquired(acquire1, NODE_2);

            NodeAllocator.NodeLease acquire1b = nodeAllocator.acquire(FULL_NODE_2_REQUIREMENTS);
            // no more space on NODE_2
            assertNotAcquired(acquire1b);
            // cancel
            acquire1b.release();

            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_3_REQUIREMENTS);
            assertAcquired(acquire2, NODE_3);
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(FULL_NODE_4_REQUIREMENTS);
            assertNotAcquired(acquire3);

            acquire1.release();
            assertEventually(() -> assertAcquired(acquire3, NODE_4));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullWithCatalogRequirements()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(nodesMapBuilder()
                .put(NODE_1, ImmutableList.of(CATALOG_1))
                .put(NODE_2, ImmutableList.of(CATALOG_1))
                .put(NODE_3, ImmutableList.of(CATALOG_2))
                .buildOrThrow());

        setupNodeAllocatorService(nodeManager, 2);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // we have 3 nodes available and per-query limit set to 2 but only 1 node that exposes CATALOG_2
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_CATALOG_2_REQUIREMENTS);
            assertAcquired(acquire1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_CATALOG_2_REQUIREMENTS);
            assertNotAcquired(acquire2);

            // releasing CATALOG_2 node allows pending lease to acquire it
            acquire1.release();
            assertEventually(() -> assertAcquired(acquire2));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullWithQueryLimitAndCatalogRequirements()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(nodesMapBuilder()
                .put(NODE_1, ImmutableList.of(CATALOG_1))
                .put(NODE_2, ImmutableList.of(CATALOG_1))
                .put(NODE_3, ImmutableList.of(CATALOG_2))
                .put(NODE_4, ImmutableList.of(CATALOG_2))
                .buildOrThrow());

        setupNodeAllocatorService(nodeManager, 2);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // allocate 2 full nodes for Q1 should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_CATALOG_1_REQUIREMENTS);
            assertAcquired(acquire1);
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_CATALOG_2_REQUIREMENTS);
            assertAcquired(acquire2);

            // another allocation for CATALOG_1 will block (per query limit is 2)
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(FULL_NODE_CATALOG_1_REQUIREMENTS);
            assertNotAcquired(acquire3);

            // releasing CATALOG_2 node for query will unblock pending lease for CATALOG_1
            acquire2.release();
            assertEventually(() -> assertAcquired(acquire3));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullNodeReleaseBeforeAcquiredWaitingOnMaxFullNodesPerQuery()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // first full allocation should not block
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);

            // next two should block (maxFullNodesPerQuery == 1)
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire2);
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire3);

            // releasing a blocked one should not unblock anything
            acquire2.release();
            assertNotAcquired(acquire3);

            // releasing an acquired one should unblock one which is still blocked
            acquire1.release();
            assertEventually(() -> assertAcquired(acquire3, NODE_1));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testAllocateFullNodeReleaseBeforeAcquiredWaitingOnOtherNodesUsed()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1));
        setupNodeAllocatorService(nodeManager, 100);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // allocate NODE_1 in shared mode
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);

            // add one more node
            addNode(nodeManager, NODE_2);

            // first full allocation should not block
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(acquire2, NODE_2);

            // next two should block (all nodes used)
            NodeAllocator.NodeLease acquire3 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire3);
            NodeAllocator.NodeLease acquire4 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(acquire4);

            // releasing a blocked one should not unblock anything
            acquire3.release();
            assertNotAcquired(acquire4);

            // releasing node acquired in shared move one should unblock one which is still blocked
            acquire1.release();
            assertEventually(() -> assertAcquired(acquire4, NODE_1));
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testRemoveAcquiredFullNode()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertAcquired(acquire1, NODE_1);

            // remove acquired node
            nodeManager.removeNode(NODE_1);

            // we should still be able to release lease for removed node
            acquire1.release();
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testNoFullNodeAvailable()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(nodesMapBuilder()
                .put(NODE_1, ImmutableList.of(CATALOG_2))
                .buildOrThrow());
        setupNodeAllocatorService(nodeManager, 100);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            // request a full node with specific catalog (not present)

            assertThatThrownBy(() -> nodeAllocator.acquire(FULL_NODE_CATALOG_1_REQUIREMENTS))
                    .hasMessage("No nodes available to run query");

            // add node with specific catalog
            addNode(nodeManager, NODE_2, CATALOG_1);

            // we should be able to acquire the node now
            NodeAllocator.NodeLease acquire1 = nodeAllocator.acquire(FULL_NODE_CATALOG_1_REQUIREMENTS);
            assertAcquired(acquire1, NODE_2);

            // acquiring one more should block (all nodes with catalog already used)
            NodeAllocator.NodeLease acquire2 = nodeAllocator.acquire(FULL_NODE_CATALOG_1_REQUIREMENTS);
            assertNotAcquired(acquire2);

            // remove node with catalog
            nodeManager.removeNode(NODE_2);
            // TODO: make FullNodeCapableNodeAllocatorService react on node removed automatically
            nodeAllocatorService.wakeupProcessPendingAcquires();

            // pending acquire2 should be completed now but with an exception
            assertEventually(() -> {
                assertFalse(acquire2.getNode().isCancelled());
                assertTrue(acquire2.getNode().isDone());
                assertThatThrownBy(() -> getFutureValue(acquire2.getNode()))
                        .hasMessage("No nodes available to run query");
            });
        }
    }

    @Test(timeOut = TEST_TIMEOUT)
    public void testRemoveAssignedFullNode()
            throws Exception
    {
        InMemoryNodeManager nodeManager = testingNodeManager(basicNodesMap(NODE_1, NODE_2));
        setupNodeAllocatorService(nodeManager, 1);

        try (NodeAllocator nodeAllocator = nodeAllocatorService.getNodeAllocator(Q1_SESSION)) {
            NodeAllocator.NodeLease sharedAcquire1 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(sharedAcquire1);
            NodeAllocator.NodeLease sharedAcquire2 = nodeAllocator.acquire(NO_REQUIREMENTS);
            assertAcquired(sharedAcquire2);

            InternalNode nodeAcquired1 = sharedAcquire1.getNode().get();
            InternalNode nodeAcquired2 = sharedAcquire2.getNode().get();
            assertNotEquals(nodeAcquired1, nodeAcquired2);

            // try to acquire full node; should not happen
            NodeAllocator.NodeLease fullAcquire = nodeAllocator.acquire(FULL_NODE_REQUIREMENTS);
            assertNotAcquired(fullAcquire);

            Set<InternalNode> pendingFullNodes = nodeAllocatorService.getPendingFullNodes();
            InternalNode pendingFullNode = Iterables.getOnlyElement(pendingFullNodes);

            // remove assigned node and release shared allocation for it; full node acquire still should not be fulfilled
            nodeManager.removeNode(pendingFullNode);
            sharedAcquire1.release();
            assertNotAcquired(fullAcquire);

            // release remaining node in the cluster
            sharedAcquire2.release();

            // full node should be fulfilled now
            assertEventually(() -> {
                // we need to wait as pending acquires are processed asynchronously
                assertAcquired(fullAcquire, nodeAcquired2);
            });
        }
    }

    private Map<InternalNode, List<CatalogName>> basicNodesMap(InternalNode... nodes)
    {
        return Arrays.stream(nodes)
                .collect(toImmutableMap(
                        node -> node,
                        node -> ALL_CATALOGS));
    }

    private ImmutableMap.Builder<InternalNode, List<CatalogName>> nodesMapBuilder()
    {
        return ImmutableMap.builder();
    }

    private void addNode(InMemoryNodeManager nodeManager, InternalNode node)
    {
        addNode(nodeManager, node, ALL_CATALOGS);
    }

    private void addNode(InMemoryNodeManager nodeManager, InternalNode node, CatalogName... catalogs)
    {
        addNode(nodeManager, node, ImmutableList.copyOf(Arrays.asList(catalogs)));
    }

    private void addNode(InMemoryNodeManager nodeManager, InternalNode node, List<CatalogName> catalogs)
    {
        checkArgument(!catalogs.isEmpty(), "no catalogs specified");
        for (CatalogName catalog : catalogs) {
            nodeManager.addNode(catalog, node);
        }
    }

    private void assertAcquired(NodeAllocator.NodeLease lease, InternalNode node)
            throws Exception
    {
        assertAcquired(lease, Optional.of(node));
    }

    private void assertAcquired(NodeAllocator.NodeLease lease)
            throws Exception
    {
        assertAcquired(lease, Optional.empty());
    }

    private void assertAcquired(NodeAllocator.NodeLease lease, Optional<InternalNode> expectedNode)
            throws Exception
    {
        assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
        assertTrue(lease.getNode().isDone(), "node lease not acquired");
        if (expectedNode.isPresent()) {
            assertEquals(lease.getNode().get(), expectedNode.get());
        }
    }

    private void assertNotAcquired(NodeAllocator.NodeLease lease)
    {
        assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
        assertFalse(lease.getNode().isDone(), "node lease acquired");
        // enforce pending acquires processing and check again
        nodeAllocatorService.processPendingAcquires();
        assertFalse(lease.getNode().isCancelled(), "node lease cancelled");
        assertFalse(lease.getNode().isDone(), "node lease acquired");
    }

    private static void assertEventually(ThrowingRunnable assertion)
    {
        Assert.assertEventually(() -> {
            try {
                assertion.run();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    interface ThrowingRunnable
    {
        void run() throws Exception;
    }
}
