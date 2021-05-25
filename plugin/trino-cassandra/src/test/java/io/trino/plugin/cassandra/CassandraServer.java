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
package io.trino.plugin.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.utility.MountableFile.forHostPath;
import static org.testng.Assert.assertEquals;

public class CassandraServer
        implements Closeable
{
    private static Logger log = Logger.get(CassandraServer.class);

    private static final int PORT = 9142;

    private static final Duration REFRESH_SIZE_ESTIMATES_TIMEOUT = new Duration(1, MINUTES);

    private final GenericContainer<?> dockerContainer;
    private final CassandraSession session;

    public CassandraServer()
            throws Exception
    {
        this("2.2");
    }

    public CassandraServer(String cassandraVersion)
            throws Exception
    {
        log.info("Starting cassandra...");

        this.dockerContainer = new GenericContainer<>("cassandra:" + cassandraVersion)
                .withExposedPorts(PORT)
                .withCopyFileToContainer(forHostPath(prepareCassandraYaml()), "/etc/cassandra/cassandra.yaml");
        this.dockerContainer.start();

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(V3)
                .withClusterName("TestCluster")
                .addContactPointsWithPorts(ImmutableList.of(
                        new InetSocketAddress(this.dockerContainer.getContainerIpAddress(), this.dockerContainer.getMappedPort(PORT))))
                .withMaxSchemaAgreementWaitSeconds(30);

        ReopeningCluster cluster = new ReopeningCluster(clusterBuilder::build);
        CassandraSession session = new CassandraSession(
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cluster,
                new Duration(1, MINUTES));

        try {
            checkConnectivity(session);
        }
        catch (RuntimeException e) {
            cluster.close();
            this.dockerContainer.stop();
            throw e;
        }

        this.session = session;
    }

    private static String prepareCassandraYaml()
            throws IOException
    {
        String original = Resources.toString(getResource("cu-cassandra.yaml"), UTF_8);

        File tempDirFile = createTempDirectory("tmp").toFile();
        tempDirFile.deleteOnExit();
        Path tmpDirPath = tempDirFile.toPath();
        Path dataDir = tmpDirPath.resolve("data");
        Files.createDirectory(dataDir);

        String modified = original.replaceAll("\\$\\{data_directory\\}", dataDir.toAbsolutePath().toString());

        Path yamlLocation = tmpDirPath.resolve("cu-cassandra.yaml");
        write(modified, yamlLocation.toFile(), UTF_8);

        return yamlLocation.toAbsolutePath().toString();
    }

    public CassandraSession getSession()
    {
        return requireNonNull(session, "session is null");
    }

    public String getHost()
    {
        return dockerContainer.getContainerIpAddress();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(PORT);
    }

    private static void checkConnectivity(CassandraSession session)
    {
        ResultSet result = session.execute("SELECT release_version FROM system.local");
        List<Row> rows = result.all();
        assertEquals(rows.size(), 1);
        String version = rows.get(0).getString(0);
        log.info("Cassandra version: %s", version);
    }

    public void refreshSizeEstimates(String keyspace, String table)
            throws Exception
    {
        long deadline = System.nanoTime() + REFRESH_SIZE_ESTIMATES_TIMEOUT.roundTo(NANOSECONDS);
        while (System.nanoTime() - deadline < 0) {
            flushTable(keyspace, table);
            refreshSizeEstimates();
            List<SizeEstimate> sizeEstimates = getSession().getSizeEstimates(keyspace, table);
            if (!sizeEstimates.isEmpty()) {
                log.info("Size estimates for the table %s.%s have been refreshed successfully: %s", keyspace, table, sizeEstimates);
                return;
            }
            log.info("Size estimates haven't been refreshed as expected. Retrying ...");
            SECONDS.sleep(1);
        }
        throw new TimeoutException(format("Attempting to refresh size estimates for table %s.%s has timed out after %s", keyspace, table, REFRESH_SIZE_ESTIMATES_TIMEOUT));
    }

    private void flushTable(String keyspace, String table)
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "flush", keyspace, table);
    }

    private void refreshSizeEstimates()
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "refreshsizeestimates");
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
