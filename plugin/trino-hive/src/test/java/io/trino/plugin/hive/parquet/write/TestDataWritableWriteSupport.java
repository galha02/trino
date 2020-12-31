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
package io.trino.plugin.hive.parquet.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

/**
 * This class is copied from org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport
 * and extended to support empty arrays and maps (HIVE-13632).
 */
class TestDataWritableWriteSupport
        extends WriteSupport<ParquetHiveRecord>
{
    private TestDataWritableWriter writer;
    private MessageType schema;
    private final boolean singleLevelArray;

    public TestDataWritableWriteSupport(boolean singleLevelArray)
    {
        this.singleLevelArray = singleLevelArray;
    }

    @Override
    public WriteContext init(Configuration configuration)
    {
        schema = DataWritableWriteSupport.getSchema(configuration);
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer)
    {
        writer = new TestDataWritableWriter(recordConsumer, schema, singleLevelArray);
    }

    @Override
    public void write(ParquetHiveRecord record)
    {
        writer.write(record);
    }
}
