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
package io.trino.tests.product.launcher.env.common;

import io.trino.testing.TestingProperties;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class Phoenix
        implements EnvironmentExtender
{
    private static final int ZOOKEEPER_PORT = 2181;

    private final PortBinder portBinder;
    private final ResourceProvider configDir;

    @Inject
    public Phoenix(DockerFiles dockerFiles, PortBinder portBinder)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("common/phoenix");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "ghcr.io/trinodb/testing/phoenix5:" + TestingProperties.getDockerImagesVersion();
        DockerContainer phoenix = new DockerContainer(dockerImageName, "phoenix")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(ZOOKEEPER_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(phoenix, ZOOKEEPER_PORT);

        builder.addContainer(phoenix);

        builder.configureTrinoContainers(container ->
                container.withCopyFileToContainer(forHostPath(configDir.getPath("hbase-site.xml")), CONTAINER_TRINO_ETC + "/hbase-site.xml"));

        configureTempto(builder, configDir);
    }
}
