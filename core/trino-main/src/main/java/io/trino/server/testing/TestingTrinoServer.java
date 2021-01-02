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
package io.trino.server.testing;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager;
import io.trino.cost.StatsCalculator;
import io.trino.dispatcher.DispatchManager;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.SqlQueryManager;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskManager;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.LocalMemoryManager;
import io.trino.metadata.AllNodes;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.GracefulShutdownHandler;
import io.trino.server.PluginManager;
import io.trino.server.ServerMainModule;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.ShutdownAction;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.SystemAccessControl;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.testing.ProcedureTester;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingEventListenerManager;
import io.trino.testing.TestingGroupProvider;
import io.trino.testing.TestingWarningCollectorModule;
import io.trino.transaction.TransactionManager;
import org.weakref.jmx.guice.MBeanModule;

import javax.annotation.concurrent.GuardedBy;
import javax.management.MBeanServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.lang.Integer.parseInt;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingTrinoServer
        implements Closeable
{
    public static TestingTrinoServer create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final Injector injector;
    private final Path baseDataDir;
    private final boolean preserveData;
    private final LifeCycleManager lifeCycleManager;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;
    private final TestingHttpServer server;
    private final CatalogManager catalogManager;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final TestingAccessControlManager accessControl;
    private final TestingGroupProvider groupProvider;
    private final ProcedureTester procedureTester;
    private final Optional<InternalResourceGroupManager<?>> resourceGroupManager;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ClusterMemoryManager clusterMemoryManager;
    private final LocalMemoryManager localMemoryManager;
    private final InternalNodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;
    private final Announcer announcer;
    private final DispatchManager dispatchManager;
    private final SqlQueryManager queryManager;
    private final TaskManager taskManager;
    private final GracefulShutdownHandler gracefulShutdownHandler;
    private final ShutdownAction shutdownAction;
    private final MBeanServer mBeanServer;
    private final boolean coordinator;

    public static class TestShutdownAction
            implements ShutdownAction
    {
        private final CountDownLatch shutdownCalled = new CountDownLatch(1);

        @GuardedBy("this")
        private boolean isWorkerShutdown;

        @Override
        public synchronized void onShutdown()
        {
            isWorkerShutdown = true;
            shutdownCalled.countDown();
        }

        public void waitForShutdownComplete(long millis)
                throws InterruptedException
        {
            shutdownCalled.await(millis, MILLISECONDS);
        }

        public synchronized boolean isWorkerShutdown()
        {
            return isWorkerShutdown;
        }
    }

    private TestingTrinoServer(
            boolean coordinator,
            Map<String, String> properties,
            Optional<String> environment,
            Optional<URI> discoveryUri,
            Module additionalModule,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls)
    {
        this.coordinator = coordinator;

        this.baseDataDir = baseDataDir.orElseGet(TestingTrinoServer::tempDirectory);
        this.preserveData = baseDataDir.isPresent();

        properties = new HashMap<>(properties);
        String coordinatorPort = properties.remove("http-server.http.port");
        if (coordinatorPort == null) {
            coordinatorPort = "0";
        }

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("coordinator", String.valueOf(coordinator))
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                .put("exchange.client-threads", "4");

        if (coordinator) {
            // TODO: enable failure detector
            serverProperties.put("failure-detector.enabled", "false");
        }

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(environment))
                .add(new TestingHttpServerModule(parseInt(coordinator ? coordinatorPort : "0")))
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new ServerMainModule("testversion"))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
                    binder.bind(EventListenerConfig.class).in(Scopes.SINGLETON);
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingGroupProvider.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(GroupProviderManager.class).in(Scopes.SINGLETON);
                    binder.bind(GroupProvider.class).to(TestingGroupProvider.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                });

        if (discoveryUri.isPresent()) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.get().toString());
            modules.add(new DiscoveryModule());
        }
        else {
            modules.add(new TestingDiscoveryModule());
        }

        modules.add(additionalModule);

        Bootstrap app = new Bootstrap(modules.build());

        Map<String, String> optionalProperties = new HashMap<>();
        environment.ifPresent(env -> optionalProperties.put("node.environment", env));

        injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties.build())
                .setOptionalConfigurationProperties(optionalProperties)
                .quiet()
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        pluginManager = injector.getInstance(PluginManager.class);

        connectorManager = injector.getInstance(ConnectorManager.class);

        server = injector.getInstance(TestingHttpServer.class);
        catalogManager = injector.getInstance(CatalogManager.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        metadata = injector.getInstance(Metadata.class);
        accessControl = injector.getInstance(TestingAccessControlManager.class);
        groupProvider = injector.getInstance(TestingGroupProvider.class);
        procedureTester = injector.getInstance(ProcedureTester.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        if (coordinator) {
            dispatchManager = injector.getInstance(DispatchManager.class);
            queryManager = (SqlQueryManager) injector.getInstance(QueryManager.class);
            resourceGroupManager = Optional.of((InternalResourceGroupManager<?>) injector.getInstance(InternalResourceGroupManager.class));
            sessionPropertyDefaults = injector.getInstance(SessionPropertyDefaults.class);
            nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
            clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
            statsCalculator = injector.getInstance(StatsCalculator.class);
            injector.getInstance(CertificateAuthenticatorManager.class).useDefaultAuthenticator();
        }
        else {
            dispatchManager = null;
            queryManager = null;
            resourceGroupManager = Optional.empty();
            sessionPropertyDefaults = null;
            nodePartitioningManager = null;
            clusterMemoryManager = null;
            statsCalculator = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        gracefulShutdownHandler = injector.getInstance(GracefulShutdownHandler.class);
        taskManager = injector.getInstance(TaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        mBeanServer = injector.getInstance(MBeanServer.class);
        announcer = injector.getInstance(Announcer.class);

        accessControl.setSystemAccessControls(systemAccessControls);

        announcer.forceAnnounce();

        refreshNodes();
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> {
                if (isDirectory(baseDataDir) && !preserveData) {
                    deleteRecursively(baseDataDir, ALLOW_INSECURE);
                }
            });

            closer.register(() -> {
                if (lifeCycleManager != null) {
                    lifeCycleManager.stop();
                }
            });
        }
    }

    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin, plugin.getClass()::getClassLoader);
    }

    public DispatchManager getDispatchManager()
    {
        return dispatchManager;
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryManager.getQueryPlan(queryId);
    }

    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        return queryManager.getFullQueryInfo(queryId);
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryManager.addFinalQueryInfoListener(queryId, stateChangeListener);
    }

    public CatalogName createCatalog(String catalogName, String connectorName)
    {
        return createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public CatalogName createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogName catalog = connectorManager.createCatalog(catalogName, connectorName, properties);
        updateConnectorIdAnnouncement(announcer, catalog, nodeManager);
        return catalog;
    }

    public Path getBaseDataDir()
    {
        return baseDataDir;
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI getHttpsBaseUrl()
    {
        return server.getHttpServerInfo().getHttpsUri();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(getBaseUrl().getHost(), getBaseUrl().getPort());
    }

    public HostAndPort getHttpsAddress()
    {
        URI httpsUri = server.getHttpServerInfo().getHttpsUri();
        return HostAndPort.fromParts(httpsUri.getHost(), httpsUri.getPort());
    }

    public CatalogManager getCatalogManager()
    {
        return catalogManager;
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public StatsCalculator getStatsCalculator()
    {
        checkState(coordinator, "not a coordinator");
        return statsCalculator;
    }

    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public TestingGroupProvider getGroupProvider()
    {
        return groupProvider;
    }

    public ProcedureTester getProcedureTester()
    {
        return procedureTester;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public Optional<InternalResourceGroupManager<?>> getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    public SessionPropertyDefaults getSessionPropertyDefaults()
    {
        return sessionPropertyDefaults;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public LocalMemoryManager getLocalMemoryManager()
    {
        return localMemoryManager;
    }

    public ClusterMemoryManager getClusterMemoryManager()
    {
        checkState(coordinator, "not a coordinator");
        return clusterMemoryManager;
    }

    public MBeanServer getMbeanServer()
    {
        return mBeanServer;
    }

    public GracefulShutdownHandler getGracefulShutdownHandler()
    {
        return gracefulShutdownHandler;
    }

    public TaskManager getTaskManager()
    {
        return taskManager;
    }

    public ShutdownAction getShutdownAction()
    {
        return shutdownAction;
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    public final AllNodes refreshNodes()
    {
        serviceSelectorManager.forceRefresh();
        nodeManager.refreshNodes();
        return nodeManager.getAllNodes();
    }

    public void waitForNodeRefresh(Duration timeout)
            throws InterruptedException, TimeoutException
    {
        Instant start = Instant.now();
        while (refreshNodes().getActiveNodes().size() < 1) {
            if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                throw new TimeoutException("Timed out while waiting for the node to refresh");
            }
            MILLISECONDS.sleep(10);
        }
    }

    public Set<InternalNode> getActiveNodesWithConnector(CatalogName catalogName)
    {
        return nodeManager.getActiveConnectorNodes(catalogName);
    }

    public <T> T getInstance(Key<T> key)
    {
        return injector.getInstance(key);
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName catalogName, InternalNodeManager nodeManager)
    {
        //
        // This code was copied from TrinoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(catalogName.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new RuntimeException("Trino announcement not found: " + announcements);
    }

    private static Path tempDirectory()
    {
        try {
            return createTempDirectory("TrinoTest");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder
    {
        private boolean coordinator = true;
        private Map<String, String> properties = ImmutableMap.of();
        private Optional<String> environment = Optional.empty();
        private Optional<URI> discoveryUri = Optional.empty();
        private Module additionalModule = EMPTY_MODULE;
        private Optional<Path> baseDataDir = Optional.empty();
        private List<SystemAccessControl> systemAccessControls = ImmutableList.of();

        public Builder setCoordinator(boolean coordinator)
        {
            this.coordinator = coordinator;
            return this;
        }

        public Builder setProperties(Map<String, String> properties)
        {
            this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
            return this;
        }

        public Builder setEnvironment(String environment)
        {
            this.environment = Optional.of(environment);
            return this;
        }

        public Builder setDiscoveryUri(URI discoveryUri)
        {
            this.discoveryUri = Optional.of(discoveryUri);
            return this;
        }

        public Builder setAdditionalModule(Module additionalModule)
        {
            this.additionalModule = requireNonNull(additionalModule, "additionalModule is null");
            return this;
        }

        public Builder setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return this;
        }

        public Builder setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
        {
            this.systemAccessControls = ImmutableList.copyOf(requireNonNull(systemAccessControls, "systemAccessControls is null"));
            return this;
        }

        public TestingTrinoServer build()
        {
            return new TestingTrinoServer(
                    coordinator,
                    properties,
                    environment,
                    discoveryUri,
                    additionalModule,
                    baseDataDir,
                    systemAccessControls);
        }
    }
}
