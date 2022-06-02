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

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.KafkaSsl;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeKafkaSsl
        extends EnvironmentProvider
{
    private final ResourceProvider configDir;

    @Inject
    public EnvMultinodeKafkaSsl(KafkaSsl kafka, MultinodeProvider multinodeProvider, DockerFiles dockerFiles)
    {
        super(multinodeProvider.singleWorker(), kafka);
        requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-kafka-ssl/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureTrinoContainers(this::addCatalogs);
        builder.addConnector("kafka");

        configureTempto(builder, configDir);
        builder.configureContainer(TESTS, container -> container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("secrets")),
                        "/docker/presto-product-tests/conf/tempto/secrets"));
    }

    private void addCatalogs(DockerContainer container)
    {
        container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("kafka_schema_registry.properties")),
                        CONTAINER_TRINO_ETC + "/catalog/kafka_schema_registry.properties")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("kafka.properties")),
                        CONTAINER_TRINO_ETC + "/catalog/kafka.properties")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("secrets")),
                        CONTAINER_TRINO_ETC + "/catalog/secrets");
    }
}
