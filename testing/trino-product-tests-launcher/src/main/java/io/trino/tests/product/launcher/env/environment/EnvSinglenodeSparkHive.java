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
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeSparkHive
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvSinglenodeSparkHive(MultinodeProvider multinodeProvider, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(multinodeProvider.singleWorker(), hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion;

        builder.configureContainer(HADOOP, dockerContainer -> {
            dockerContainer.setDockerImageName(dockerImageName);
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-hdp3/apply-hdp3-config.sh")),
                    CONTAINER_HADOOP_INIT_D + "apply-hdp3-config.sh");
        });

        builder.configureTests(dockerContainer -> {
            dockerContainer.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/tempto/tempto-configuration-for-hive3.yaml")),
                    CONTAINER_TEMPTO_PROFILE_CONFIG);
        });

        builder.addConnector("hive", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-hive/hive.properties")));

        builder.addContainer(createSpark())
                .containerDependsOn("spark", HADOOP);
    }

    @SuppressWarnings("resource")
    private DockerContainer createSpark()
    {
        // TODO: Switch to pure Spark 3 image once it's available (https://github.com/trinodb/trino/issues/7063)
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3.0-iceberg:" + hadoopImagesVersion, "spark")
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-hive/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCommand(
                        "spark-submit",
                        "--master", "local[*]",
                        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                        "--name", "Thrift JDBC/ODBC Server",
                        "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                        "spark-internal")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }
}
