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
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.kafka.util.TestingKafka;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import kafka.producer.KeyedMessage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import java.time.Instant;

import java.util.UUID;

import static io.prestosql.plugin.kafka.KafkaQueryRunner.createKafkaQueryRunner;
import static io.prestosql.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.prestosql.plugin.kafka.util.TestingKafka.CloseableProducer;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private TestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        QueryRunner queryRunner = createKafkaQueryRunner(
                testingKafka,
                ImmutableList.of(),
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build());
        testingKafka.createTopics(topicName);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafka()
    {
        testingKafka.close();
        testingKafka = null;
    }

    @Test
    public void testTopicExists()
    {
        assertTrue(getQueryRunner().listTables(getSession(), "kafka", "default").contains(QualifiedObjectName.valueOf("kafka.default." + topicName)));
    }

    @Test
    public void testTopicHasData()
    {
        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 0");

        createMessages(topicName);

        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 100000L");
    }

    private void createMessages(String topicName)
    {
        try (CloseableProducer<Long, Object> producer = testingKafka.createProducer()) {
            int jMax = 10_000;
            int iMax = 100_000 / jMax;
            for (long i = 0; i < iMax; i++) {
                ImmutableList.Builder<KeyedMessage<Long, Object>> builder = ImmutableList.builder();
                for (long j = 0; j < jMax; j++) {
                    builder.add(new KeyedMessage<>(topicName, i, ImmutableMap.of("id", Long.toString(i * iMax + j), "value", UUID.randomUUID().toString())));
                }
                producer.send(builder.build());
            }
        }
    }

    @Test
    public void testTopicMetadata() {
        long currentTimestamp = Instant.now().toEpochMilli();

        createMessages(topicName);

        MaterializedResult result = getQueryRunner().execute(getSession(), "SELECT max(_timestamp) FROM default." + topicName);

        assertTrue(result.getMaterializedRows().size() > 0);

        MaterializedRow row = result.getMaterializedRows().get(0);
        long messageCreationTimestamp = (Long) row.getField(0);

        assertTrue(messageCreationTimestamp >= currentTimestamp);
    }
}
