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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeHiveCaching
        extends EnvironmentProvider
{
    public static final String CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES = CONTAINER_TRINO_ETC + "/catalog/hivenoncached.properties";

    private final DockerFiles dockerFiles;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeHiveCaching(
            DockerFiles dockerFiles,
            MultinodeProvider multinodeProvider,
            Hadoop hadoop)
    {
        super(ImmutableList.of(multinodeProvider.workers(2), hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureCoordinator(container -> container.withTmpFs(ImmutableMap.of("/tmp/cache", "rw")));
        builder.addConnector("hive", forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES);
        builder.addConnector("hive", forHostPath(configDir.getPath("multinode-cached/hive-coordinator.properties")), CONTAINER_TRINO_HIVE_PROPERTIES);

        builder.configureWorkers(workerContainer -> workerContainer
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("common/hadoop/hive.properties")), CONTAINER_PRESTO_HIVE_NON_CACHED_PROPERTIES)
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-cached/hive-worker.properties")), CONTAINER_TRINO_HIVE_PROPERTIES)
                .withTmpFs(ImmutableMap.of("/tmp/cache", "rw")));
    }
}
