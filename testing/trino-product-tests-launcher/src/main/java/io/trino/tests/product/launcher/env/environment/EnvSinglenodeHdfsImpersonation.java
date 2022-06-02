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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.MultinodeProvider;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvSinglenodeHdfsImpersonation
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    @Inject
    public EnvSinglenodeHdfsImpersonation(DockerFiles dockerFiles, MultinodeProvider multinodeProvider, Hadoop hadoop)
    {
        super(ImmutableList.of(multinodeProvider.singleWorker(), hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("hive", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-hdfs-impersonation/hive.properties")));
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-hdfs-impersonation/iceberg.properties")));
    }
}
