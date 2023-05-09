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
package io.trino.filesystem;

import com.google.common.collect.ImmutableMap;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.security.ConnectorIdentity;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_EXISTS;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_LOCATION;
import static java.util.Objects.requireNonNull;

public class TrackingFileSystemFactory
        implements TrinoFileSystemFactory
{
    public enum OperationType
    {
        INPUT_FILE_GET_LENGTH,
        INPUT_FILE_NEW_STREAM,
        INPUT_FILE_EXISTS,
        OUTPUT_FILE_CREATE,
        OUTPUT_FILE_CREATE_OR_OVERWRITE,
        OUTPUT_FILE_LOCATION,
        OUTPUT_FILE_TO_INPUT_FILE,
    }

    private final AtomicInteger fileId = new AtomicInteger();
    private final TrinoFileSystemFactory delegate;

    private final Map<OperationContext, Integer> operationCounts = new ConcurrentHashMap<>();

    public TrackingFileSystemFactory(TrinoFileSystemFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public Map<OperationContext, Integer> getOperationCounts()
    {
        return ImmutableMap.copyOf(operationCounts);
    }

    public void reset()
    {
        operationCounts.clear();
    }

    private void increment(Location path, int fileId, OperationType operationType)
    {
        OperationContext context = new OperationContext(path, fileId, operationType);
        operationCounts.merge(context, 1, Math::addExact);    // merge is atomic for ConcurrentHashMap
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new TrackingFileSystem(delegate.create(identity), this::increment);
    }

    private interface Tracker
    {
        void track(Location path, int fileId, OperationType operationType);
    }

    private class TrackingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;
        private final Tracker tracker;

        private TrackingFileSystem(TrinoFileSystem delegate, Tracker tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingInputFile(
                    delegate.newInputFile(location),
                    operation -> tracker.track(location, nextId, operation));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingInputFile(
                    delegate.newInputFile(location, length),
                    operation -> tracker.track(location, nextId, operation));
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingOutputFile(
                    delegate.newOutputFile(location),
                    operationType -> tracker.track(location, nextId, operationType));
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            delegate.deleteFile(location);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            delegate.deleteFiles(locations);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            delegate.deleteDirectory(location);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            delegate.renameFile(source, target);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            return delegate.listFiles(location);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            return delegate.directoryExists(location);
        }
    }

    private static class TrackingInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingInputFile(TrinoInputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public long length()
                throws IOException
        {
            tracker.accept(INPUT_FILE_GET_LENGTH);
            return delegate.length();
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            tracker.accept(INPUT_FILE_NEW_STREAM);
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            tracker.accept(INPUT_FILE_NEW_STREAM);
            return delegate.newStream();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            tracker.accept(INPUT_FILE_EXISTS);
            return delegate.exists();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }
    }

    private static class TrackingOutputFile
            implements TrinoOutputFile
    {
        private final TrinoOutputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingOutputFile(TrinoOutputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delete is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            tracker.accept(OUTPUT_FILE_CREATE);
            return delegate.create(memoryContext);
        }

        @Override
        public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            tracker.accept(OUTPUT_FILE_CREATE_OR_OVERWRITE);
            return delegate.createOrOverwrite(memoryContext);
        }

        @Override
        public Location location()
        {
            tracker.accept(OUTPUT_FILE_LOCATION);
            return delegate.location();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }
    }

    @Immutable
    public static class OperationContext
    {
        private final Location location;
        private final int fileId;
        private final OperationType operationType;

        public OperationContext(Location location, int fileId, OperationType operationType)
        {
            this.location = requireNonNull(location, "location is null");
            this.fileId = fileId;
            this.operationType = requireNonNull(operationType, "operationType is null");
        }

        public Location getLocation()
        {
            return location;
        }

        public int getFileId()
        {
            return fileId;
        }

        public OperationType getOperationType()
        {
            return operationType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OperationContext that = (OperationContext) o;
            return Objects.equals(location, that.location)
                    && fileId == that.fileId
                    && operationType == that.operationType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(location, fileId, operationType);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("path", location)
                    .add("fileId", fileId)
                    .add("operation", operationType)
                    .toString();
        }
    }
}
