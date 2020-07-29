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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.orc.OrcFileWriterFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.DataOperationType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.AcidSchema.ACID_COLUMN_NAMES;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.hadoop.hive.ql.io.AcidUtils.OrcAcidVersion.writeVersionFile;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;

public class HiveUpdatablePageSource
        implements UpdatablePageSource
{
    // The channel numbers of the child blocks in the RowBlock passed to deleteRows()
    public static final int ORIGINAL_TRANSACTION_CHANNEL = 0;
    public static final int ROW_ID_CHANNEL = 1;
    public static final int BUCKET_CHANNEL = 2;
    public static final int ACID_ROW_STRUCT_COLUMN_ID = 6;

    // The bucketPath looks like .../delta_nnnnnnn_mmmmmmm_ssss/bucket_bbbbb(_aaaa)?
    public static final Pattern BUCKET_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>delta_[\\d]+_[\\d]+)_(?<statementId>[\\d]+)/(?<filename>bucket_(?<bucketNumber>[\\d]+)(?<attemptId>_[\\d]+)?)$");

    private final HiveTableHandle hiveTable;
    private final String partitionName;
    private final ConnectorPageSource hivePageSource;
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final Configuration configuration;
    private final ConnectorSession session;
    private final HiveType hiveRowType;
    private final long writeId;
    private final Properties hiveAcidSchema;
    private final Path deleteDeltaDirectory;
    private final String bucketFilename;

    private Optional<FileWriter> writer = Optional.empty();
    private boolean closed;

    public HiveUpdatablePageSource(
            HiveTableHandle hiveTableHandle,
            String partitionName,
            int statementId,
            ConnectorPageSource hivePageSource,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            Path bucketPath,
            OrcFileWriterFactory orcFileWriterFactory,
            Configuration configuration,
            ConnectorSession session,
            HiveType hiveRowType)
    {
        this.hiveTable = requireNonNull(hiveTableHandle, "hiveTable is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.hivePageSource = requireNonNull(hivePageSource, "hivePageSource is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = hdfsEnvironment;
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.session = requireNonNull(session, "session is null");
        this.hiveRowType = requireNonNull(hiveRowType, "rowType is null");
        checkArgument(hiveTableHandle.isInAcidTransaction(), "Not in a transaction; hiveTableHandle: " + hiveTableHandle);
        this.writeId = hiveTableHandle.getWriteId();
        this.hiveAcidSchema = AcidSchema.createAcidSchema(hiveRowType);
        requireNonNull(bucketPath, "bucketPath is null");
        Matcher matcher = BUCKET_PATH_MATCHER.matcher(bucketPath.toString());
        checkArgument(matcher.matches(), "bucketPath doesn't have the required format: " + bucketPath);
        this.bucketFilename = matcher.group("filename");
        this.deleteDeltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deleteDeltaSubdir(writeId, writeId, statementId)));
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        int positionCount = rowIds.getPositionCount();
        List<Block> blocks = rowIds.getChildren();
        checkArgument(blocks.size() == 3, "The rowId block should have 3 children, but has " + blocks.size());
        Block[] blockArray = new Block[] {
                RunLengthEncodedBlock.create(INTEGER, (long) DataOperationType.DELETE.getValue(), positionCount),
                blocks.get(ORIGINAL_TRANSACTION_CHANNEL),
                blocks.get(BUCKET_CHANNEL),
                blocks.get(ROW_ID_CHANNEL),
                RunLengthEncodedBlock.create(BIGINT, writeId, positionCount),
                RunLengthEncodedBlock.create(hiveRowType.getType(typeManager), null, positionCount)
        };
        Page deletePage = new Page(blockArray);

        lazyInitializeFileWriter();
        checkArgument(writer.isPresent(), "writer not present");
        writer.get().appendRows(deletePage);
    }

    private void lazyInitializeFileWriter()
    {
        if (writer.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            writer = orcFileWriterFactory.createFileWriter(
                    new Path(format("%s/%s", deleteDeltaDirectory, bucketFilename)),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    hiveTable.getTransaction());
        }
    }

    private void createOrcAcidVersionFile()
    {
        try {
            FileSystem fs = hdfsEnvironment.getFileSystem(new HdfsContext(session, hiveTable.getSchemaName()), deleteDeltaDirectory);
            writeVersionFile(deleteDeltaDirectory, fs);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Exception writing _orc_acid_version file for deltaDirectory " + deleteDeltaDirectory, e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (writer.isPresent()) {
            writer.get().commit();
            createOrcAcidVersionFile();
            return completedFuture(ImmutableList.of(Slices.utf8Slice(partitionName)));
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public long getCompletedBytes()
    {
        return hivePageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return hivePageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page = hivePageSource.getNextPage();
        if (page == null) {
            close();
            return null;
        }
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return hivePageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            hivePageSource.close();
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, e);
        }
    }
}
