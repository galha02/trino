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
package io.prestosql.tests.product.launcher.env;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.InvocationBuilder;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.log.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DockerContainer
        extends FixedHostPortGenericContainer<DockerContainer>
{
    private static final Logger log = Logger.get(DockerContainer.class);

    private String logicalName;
    private List<String> logPaths = new ArrayList<>();
    private Optional<EnvironmentListener> listener = Optional.empty();

    public DockerContainer(String dockerImageName, String logicalName)
    {
        super(dockerImageName);
        this.logicalName = requireNonNull(logicalName, "logicalName is null");

        // workaround for https://github.com/testcontainers/testcontainers-java/pull/2861
        setCopyToFileContainerPathMap(new LinkedHashMap<>());
    }

    public String getLogicalName()
    {
        return logicalName;
    }

    public DockerContainer withEnvironmentListener(Optional<EnvironmentListener> listener)
    {
        this.listener = requireNonNull(listener, "listener is null");
        return this;
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode, selinuxContext);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void copyFileToContainer(MountableFile mountableFile, String containerPath)
    {
        verifyHostPath(mountableFile.getResolvedPath());
        copyFileToContainer(containerPath, () -> super.copyFileToContainer(mountableFile, containerPath));
    }

    @Override
    public void copyFileToContainer(Transferable transferable, String containerPath)
    {
        copyFileToContainer(containerPath, () -> super.copyFileToContainer(transferable, containerPath));
    }

    public DockerContainer withExposedLogPaths(String... logPaths)
    {
        requireNonNull(this.logPaths, "log paths are already exposed");
        this.logPaths.addAll(Arrays.asList(logPaths));
        return this;
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo)
    {
        super.containerIsStarting(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStarting(this, containerInfo));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo)
    {
        super.containerIsStarted(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStarted(this, containerInfo));
    }

    @Override
    protected void containerIsStopping(InspectContainerResponse containerInfo)
    {
        super.containerIsStopping(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStopping(this, containerInfo));
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo)
    {
        super.containerIsStopped(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStopped(this, containerInfo));
    }

    private void copyFileToContainer(String containerPath, Runnable copy)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        copy.run();
        log.info("Copied files into %s %s in %.1f s", this, containerPath, stopwatch.elapsed(MILLISECONDS) / 1000.);
    }

    public void clearDependencies()
    {
        dependencies.clear();
    }

    public void copyLogsToHostPath(Path hostPath)
    {
        if (!isRunning()) {
            log.warn("Could not copy files from stopped container %s", logicalName);
            return;
        }

        log.info("Copying container %s logs to '%s'", logicalName, hostPath);

        Path hostLogPath = Paths.get(hostPath.toString(), logicalName);
        ensurePathExists(hostLogPath);

        for (String containerLogPath : logPaths) {
            try {
                listFilesInContainer(containerLogPath).forEach(filename ->
                        copyFileFromContainer(filename, hostLogPath));
            }
            catch (Exception e) {
                log.warn("Could not copy logs from %s to '%s': %s", logicalName, hostPath, e);
            }
        }
    }

    private void copyFileFromContainer(String filename, Path rootHostPath)
    {
        Path targetPath = rootHostPath.resolve(filename.replaceFirst("^\\/", ""));

        log.info("Copying file %s to %s", filename, targetPath);
        ensurePathExists(targetPath.getParent());
        copyFileFromContainer(filename, targetPath.toString());
    }

    private Stream<String> listFilesInContainer(String path)
    {
        try {
            ExecResult result = execInContainer("/usr/bin/find", path, "-type", "f", "-print");

            if (result.getExitCode() == 0L) {
                return Splitter.on("\n")
                        .omitEmptyStrings()
                        .splitToStream(result.getStdout());
            }

            throw new RuntimeException(format("Could not list files in %s: %s", path, result.getStderr()));
        }
        catch (Exception e) {
            log.warn("Could not list files in container '%s': %s", logicalName, e);
        }

        return Stream.empty();
    }

    public Statistics getStats()
    {
        try (DockerClient client = DockerClientFactory.lazyClient()) {
            InvocationBuilder.AsyncResultCallback<Statistics> callback = new InvocationBuilder.AsyncResultCallback<>();
            client.statsCmd(getContainerId()).exec(callback);
            return callback.awaitResult();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return logicalName;
    }

    // Mounting a non-existing file results in docker creating a directory. This is often not the desired effect. Fail fast instead.
    private static void verifyHostPath(String hostPath)
    {
        if (!Files.exists(Paths.get(hostPath))) {
            throw new IllegalArgumentException("Host path does not exist: " + hostPath);
        }
    }

    public static void cleanOrCreateHostPath(Path path)
    {
        try {
            if (Files.exists(path)) {
                deleteRecursively(path, RecursiveDeleteOption.ALLOW_INSECURE);
                log.info("Removed host directory: '%s'", path);
            }

            ensurePathExists(path);
            log.info("Created host directory: '%s'", path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void ensurePathExists(Path path)
    {
        try {
            Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public enum OutputMode
    {
        PRINT,
        DISCARD,
        WRITE,
        PRINT_WRITE,
        /**/;
    }
}
