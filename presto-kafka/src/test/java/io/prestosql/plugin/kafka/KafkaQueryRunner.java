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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.decoder.DecoderModule;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.kafka.schema.ContentSchemaReader;
import io.prestosql.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.prestosql.plugin.kafka.schema.TableDescriptionSupplier;
import io.prestosql.plugin.kafka.schema.file.FileContentSchemaReader;
import io.prestosql.plugin.kafka.util.CodecSupplier;
import io.prestosql.plugin.kafka.util.TestUtils;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.TestingPrestoClient;
import io.prestosql.testing.kafka.BasicTestingKafka;
import io.prestosql.testing.kafka.TestingKafka;
import io.prestosql.tpch.TpchTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.ByteStreams.toByteArray;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.kafka.util.TestUtils.loadTpchTopicDescription;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner() {}

    private static final Logger log = Logger.get(KafkaQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";
    private static final String TEST = "test";

    public static Builder builder(BasicTestingKafka testingKafka)
    {
        return new Builder(testingKafka);
    }

    public static class Builder
            extends KafkaQueryRunnerBuilder<BasicTestingKafka>
    {
        private List<TpchTable<?>> tables = ImmutableList.of();
        private Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription = ImmutableMap.of();

        protected Builder(BasicTestingKafka testingKafka)
        {
            super(testingKafka, TPCH_SCHEMA);
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        public Builder setExtraTopicDescription(Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
        {
            this.extraTopicDescription = ImmutableMap.copyOf(requireNonNull(extraTopicDescription, "extraTopicDescription is null"));
            return this;
        }

        @Override
        public Builder setExtension(Module extension)
        {
            this.extension = requireNonNull(extension, "extension is null");
            return this;
        }

        @Override
        public void preInit(DistributedQueryRunner queryRunner)
                throws Exception
        {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            for (TpchTable<?> table : tables) {
                testingKafka.createTopic(kafkaTopicName(table));
            }
            Map<SchemaTableName, KafkaTopicDescription> tpchTopicDescriptions = createTpchTopicDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            List<SchemaTableName> tableNames = new ArrayList<>();
            tableNames.add(new SchemaTableName("read_test", "all_datatypes_json"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_avro"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_csv"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_raw"));
            tableNames.add(new SchemaTableName("write_test", "all_datatypes_json"));

            JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, queryRunner.getMetadata()).get();

            ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> testTopicDescriptions = ImmutableMap.builder();
            for (SchemaTableName tableName : tableNames) {
                testTopicDescriptions.put(tableName, createTable(tableName, testingKafka, topicDescriptionJsonCodec));
            }

            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                    .putAll(extraTopicDescription)
                    .putAll(tpchTopicDescriptions)
                    .putAll(testTopicDescriptions.build())
                    .build();
            addModules(installModuleIf(
                    KafkaConfig.class,
                    kafkaConfig -> kafkaConfig.getTableDescriptionSupplier().equalsIgnoreCase(TEST),
                    binder -> binder.bind(TableDescriptionSupplier.class)
                            .toInstance(new MapBasedTableDescriptionSupplier(topicDescriptions))),
                    binder -> binder.bind(ContentSchemaReader.class).to(FileContentSchemaReader.class).in(Scopes.SINGLETON),
                    new DecoderModule());
            Map<String, String> properties = new HashMap<>(extraKafkaProperties);
            properties.putIfAbsent("kafka.table-description-supplier", TEST);
            setExtraKafkaProperties(properties);
        }

        @Override
        public void postInit(DistributedQueryRunner queryRunner)
        {
            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(testingKafka, prestoClient, table);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
        }
    }

    private static KafkaTopicDescription createTable(SchemaTableName table, TestingKafka testingKafka, JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec)
            throws IOException
    {
        testingKafka.createTopic(table.toString());
        String fileName = format("/%s/%s.json", table.getSchemaName(), table.getTableName());
        KafkaTopicDescription tableTemplate = topicDescriptionJsonCodec.fromJson(toByteArray(KafkaQueryRunner.class.getResourceAsStream(fileName)));

        Optional<KafkaTopicFieldGroup> key = tableTemplate.getKey()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.getFields()));

        Optional<KafkaTopicFieldGroup> message = tableTemplate.getMessage()
                .map(keyTemplate -> new KafkaTopicFieldGroup(
                        keyTemplate.getDataFormat(),
                        keyTemplate.getDataSchema().map(schema -> KafkaQueryRunner.class.getResource(schema).getPath()),
                        Optional.empty(),
                        keyTemplate.getFields()));

        return new KafkaTopicDescription(
                table.getTableName(),
                Optional.of(table.getSchemaName()),
                table.toString(),
                key,
                message);
    }

    private static void loadTpchTopic(TestingKafka testingKafka, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        TestUtils.loadTpchTopic(testingKafka, prestoClient, kafkaTopicName(table), new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String kafkaTopicName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + "." + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, KafkaTopicDescription> createTpchTopicDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, metadata).get();

        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            topicDescriptions.put(loadTpchTopicDescription(topicDescriptionJsonCodec, tpchTable.toString(), tpchTable));
        }
        return topicDescriptions.build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = builder(new BasicTestingKafka())
                .setTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
