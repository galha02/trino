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
package io.trino.tests.product.hive;

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import org.testng.annotations.Test;

import java.nio.file.Paths;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PARQUET;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestParquetInt64MillisTimestampColumn
        extends HiveProductTest
{
    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestParquetInt64MillisTimestampColumn/metadata-int64");
        saveOnHdfs(
                Paths.get("/docker/presto-product-tests/parquet/int64_timestamp_annotated.parquet"),
                "/user/hive/warehouse/TestParquetInt64MillisTimestampColumn/metadata-int64/int64_timestamp_annotated.parquet");

        hdfsClient.createDirectory("/user/hive/warehouse/TestParquetInt64MillisTimestampColumn/plain-int64");
        saveOnHdfs(
                Paths.get("/docker/presto-product-tests/parquet/int64_timestamp_plain.parquet"),
                "/user/hive/warehouse/TestParquetInt64MillisTimestampColumn/plain-int64/int64_timestamp_plain.parquet");
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestParquetInt64MillisTimestampColumn");
    }

    @Test(groups = {PARQUET, STORAGE_FORMATS})
    public void testSelectAnnotatedInt64MillisTimestamp()
    {
         /*
         message spark_schema {
           optional int64 ts (TIMESTAMP(MILLIS,true));
           optional binary id (STRING);
         }
          */
        onTrino().executeQuery("CREATE TABLE events_int64_meta (\n" +
                "id varchar, ts timestamp(3)" +
                ") WITH (\n" +
                "external_location = 'hdfs:///user/hive/warehouse/TestParquetInt64MillisTimestampColumn/metadata-int64',\n" +
                "format = 'PARQUET');");

        assertThat(onTrino().executeQuery("SELECT id, ts FROM events_int64_meta LIMIT 3"))
                .containsOnly(
                        row("5ab115df-c3cf-470e-966b-66c96f4d2004", "2021-09-20 08:49:46.531"),
                        row("dec75266-3293-471c-9023-74693eb1247e", "2021-09-20 08:49:46.531"),
                        row("a4d7956f-47dc-4674-b1af-f335f6c9ddfc", "2021-09-20 08:49:46.531"));

        onTrino().executeQuery("DROP TABLE events_int64_meta");
    }

    @Test(groups = {PARQUET, STORAGE_FORMATS})
    public void testSelectPlainInt64MillisTimestamp()
    {
        /*
        message hive_schema {
          optional int64 ts;
          optional binary id (STRING);
        }
         */
        onTrino().executeQuery("CREATE TABLE events_int64_plain (\n" +
                "id varchar, ts timestamp(3)" +
                ") WITH (\n" +
                "external_location = 'hdfs:///user/hive/warehouse/TestParquetInt64MillisTimestampColumn/plain-int64',\n" +
                "format = 'PARQUET');");

        assertThat(onTrino().executeQuery("SELECT id, ts FROM events_int64_plain LIMIT 3"))
                .containsOnly(
                        row("5ab115df-c3cf-470e-966b-66c96f4d2004", "2021-09-20 08:49:46.531"),
                        row("dec75266-3293-471c-9023-74693eb1247e", "2021-09-20 08:49:46.531"),
                        row("a4d7956f-47dc-4674-b1af-f335f6c9ddfc", "2021-09-20 08:49:46.531"));

        onTrino().executeQuery("DROP TABLE events_int64_plain");
    }
}
