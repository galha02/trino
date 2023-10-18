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
package io.trino.plugin.exchange.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HadoopFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private final int blockSize;
    private final ExchangeHdfsEnvironment exchangeHdfsEnvironment;

    @Inject
    public HadoopFileSystemExchangeStorage(ExchangeHdfsConfig config, ExchangeHdfsEnvironment exchangeHdfsEnvironment)
    {
        this.exchangeHdfsEnvironment = requireNonNull(exchangeHdfsEnvironment, "exchangeHdfsEnvironment is null");
        blockSize = toIntExact(config.getHdfsStorageBlockSize().toBytes());
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        Path path = new Path(dir);
        FileSystem fileSystem = exchangeHdfsEnvironment.getFileSystem(path);
        fileSystem.mkdirs(path);
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new HadoopExchangeStorageReader(exchangeHdfsEnvironment, sourceFiles, blockSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        return new HadoopExchangeStorageWriter(exchangeHdfsEnvironment, file);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        try {
            Path path = new Path(file);
            FileSystem fileSystem = exchangeHdfsEnvironment.getFileSystem(path);
            fileSystem.createNewFile(path);
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        for (URI dir : directories) {
            try {
                Path path = new Path(dir);
                FileSystem fileSystem = exchangeHdfsEnvironment.getFileSystem(path);
                fileSystem.delete(path, true);
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
        try {
            Path path = new Path(dir);
            FileSystem fileSystem = exchangeHdfsEnvironment.getFileSystem(path);
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                    path, true);
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                builder.add(new FileStatus(fileStatus.getPath().toString(), fileStatus.getLen()));
            }
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(builder.build());
    }

    @Override
    public int getWriteBufferSize()
    {
        return blockSize;
    }

    @Override
    public void close()
    {
    }

    @ThreadSafe
    private static class HadoopExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(HadoopExchangeStorageReader.class);

        private final ExchangeHdfsEnvironment exchangeHdfsEnvironment;
        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final int blockSize;

        @GuardedBy("this")
        private InputStreamSliceInput sliceInput;
        @GuardedBy("this")
        private boolean closed;

        public HadoopExchangeStorageReader(ExchangeHdfsEnvironment exchangeHdfsEnvironment, List<ExchangeSourceFile> sourceFiles, int blockSize)
        {
            this.exchangeHdfsEnvironment = requireNonNull(exchangeHdfsEnvironment, "exchangeHdfsEnvironment is null");
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
            this.blockSize = blockSize;
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed) {
                return null;
            }

            if (sliceInput != null) {
                if (sliceInput.isReadable()) {
                    return sliceInput.readSlice(sliceInput.readInt());
                }
                else {
                    sliceInput.close();
                }
            }

            ExchangeSourceFile sourceFile = sourceFiles.poll();
            if (sourceFile == null) {
                close();
                return null;
            }

            sliceInput = getSliceInput(sourceFile);
            return sliceInput.readSlice(sliceInput.readInt());
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + (sliceInput == null ? 0 : sliceInput.getRetainedSize());
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (sliceInput != null) {
                sliceInput.close();
                sliceInput = null;
            }
        }

        private InputStreamSliceInput getSliceInput(ExchangeSourceFile sourceFile)
                throws IOException
        {
            Path fileURL = new Path(sourceFile.getFileUri());
            return new InputStreamSliceInput(exchangeHdfsEnvironment.getFileSystem(fileURL).open(fileURL), blockSize);
        }
    }

    @NotThreadSafe
    private static class HadoopExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(HadoopExchangeStorageReader.class);
        private final OutputStream outputStream;

        public HadoopExchangeStorageWriter(ExchangeHdfsEnvironment exchangeHdfsEnvironment, URI file)
        {
            try {
                Path path = new Path(file);
                this.outputStream = exchangeHdfsEnvironment.getFileSystem(path).create(path, true);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            try {
                outputStream.write(slice.getBytes());
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}
