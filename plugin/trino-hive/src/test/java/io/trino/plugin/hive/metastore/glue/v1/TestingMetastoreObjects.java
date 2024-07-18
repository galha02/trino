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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TableType;
import io.trino.plugin.hive.metastore.HiveType;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.security.PrincipalType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.Table.TABLE_COMMENT;
import static java.lang.String.format;

public final class TestingMetastoreObjects
{
    private TestingMetastoreObjects() {}

    // --------------- Glue Objects ---------------

    public static Database getGlueTestDatabase()
    {
        return new Database()
                .withName("test-db" + generateRandom())
                .withDescription("database desc")
                .withLocationUri("/db")
                .withParameters(ImmutableMap.of());
    }

    public static Table getGlueTestTable(String dbName)
    {
        return new Table()
                .withDatabaseName(dbName)
                .withName("test-tbl" + generateRandom())
                .withOwner("owner")
                .withParameters(ImmutableMap.of())
                .withPartitionKeys(ImmutableList.of(getGlueTestColumn()))
                .withStorageDescriptor(getGlueTestStorageDescriptor())
                .withTableType(EXTERNAL_TABLE.name())
                .withViewOriginalText("originalText")
                .withViewExpandedText("expandedText");
    }

    public static Table getGlueTestTrinoMaterializedView(String dbName)
    {
        return new Table()
                .withDatabaseName(dbName)
                .withName("test-mv" + generateRandom())
                .withOwner("owner")
                .withParameters(ImmutableMap.of(PRESTO_VIEW_FLAG, "true", TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT))
                .withPartitionKeys()
                .withStorageDescriptor(null)
                .withTableType(TableType.VIRTUAL_VIEW.name())
                .withViewOriginalText("/* %s: base64encodedquery */".formatted(ICEBERG_MATERIALIZED_VIEW_COMMENT))
                .withViewExpandedText(ICEBERG_MATERIALIZED_VIEW_COMMENT);
    }

    public static Column getGlueTestColumn()
    {
        return getGlueTestColumn("string");
    }

    public static Column getGlueTestColumn(String type)
    {
        return new Column()
                .withName("test-col" + generateRandom())
                .withType(type)
                .withComment("column comment");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor()
    {
        return getGlueTestStorageDescriptor(ImmutableList.of(getGlueTestColumn()), "SerdeLib");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor(List<Column> columns, String serde)
    {
        return new StorageDescriptor()
                .withBucketColumns(ImmutableList.of("test-bucket-col"))
                .withColumns(columns)
                .withParameters(ImmutableMap.of())
                .withSerdeInfo(new SerDeInfo()
                        .withSerializationLibrary(serde)
                        .withParameters(ImmutableMap.of()))
                .withInputFormat("InputFormat")
                .withOutputFormat("OutputFormat")
                .withLocation("/test-tbl")
                .withNumberOfBuckets(1);
    }

    public static Partition getGlueTestPartition(String dbName, String tblName, List<String> values)
    {
        return new Partition()
                .withDatabaseName(dbName)
                .withTableName(tblName)
                .withValues(values)
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(getGlueTestStorageDescriptor());
    }

    // --------------- Trino Objects ---------------

    public static io.trino.plugin.hive.metastore.Database getTrinoTestDatabase()
    {
        return io.trino.plugin.hive.metastore.Database.builder()
                .setDatabaseName("test-db" + generateRandom())
                .setComment(Optional.of("database desc"))
                .setLocation(Optional.of("/db"))
                .setParameters(ImmutableMap.of())
                .setOwnerName(Optional.of("PUBLIC"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    public static io.trino.plugin.hive.metastore.Table getTrinoTestTable(String dbName)
    {
        return io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(dbName)
                .setTableName("test-tbl" + generateRandom())
                .setOwner(Optional.of("owner"))
                .setParameters(ImmutableMap.of())
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setDataColumns(ImmutableList.of(getTrinoTestColumn()))
                .setPartitionColumns(ImmutableList.of(getTrinoTestColumn()))
                .setViewOriginalText(Optional.of("originalText"))
                .setViewExpandedText(Optional.of("expandedText"))
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static io.trino.plugin.hive.metastore.Partition getTrinoTestPartition(String dbName, String tblName, List<String> values)
    {
        return io.trino.plugin.hive.metastore.Partition.builder()
                .setDatabaseName(dbName)
                .setTableName(tblName)
                .setValues(values)
                .setColumns(ImmutableList.of(getTrinoTestColumn()))
                .setParameters(ImmutableMap.of())
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static io.trino.plugin.hive.metastore.Column getTrinoTestColumn()
    {
        return new io.trino.plugin.hive.metastore.Column("test-col" + generateRandom(), HiveType.HIVE_STRING, Optional.of("column comment"), Map.of());
    }

    private static final Consumer<Storage.Builder> STORAGE_CONSUMER = storage ->
            storage.setStorageFormat(StorageFormat.create("SerdeLib", "InputFormat", "OutputFormat"))
                    .setLocation("/test-tbl")
                    .setBucketProperty(Optional.empty())
                    .setSerdeParameters(ImmutableMap.of());

    private static String generateRandom()
    {
        return format("%04x", ThreadLocalRandom.current().nextInt());
    }
}
