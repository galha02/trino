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
package io.trino.plugin.kafka;

import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;

public class TestKafkaDistributedLatest
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka testingKafka = closeAfterClass(TestingKafka.create("6.0.1"));
        return KafkaQueryRunner.builder(testingKafka)
                .setTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
