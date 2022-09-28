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
package io.trino.plugin.raptor.legacy.metadata;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.sql.Timestamp;
import java.util.UUID;

public interface MySqlShardDao
        extends ShardDao
{
    @Override
    @SqlUpdate("DELETE x\n" +
            "FROM shard_nodes x\n" +
            "JOIN shards USING (shard_id)\n" +
            "WHERE table_id = :tableId")
    void dropShardNodes(long tableId);

    @Override
    @SqlBatch("INSERT IGNORE INTO deleted_shards (shard_uuid, delete_time)\n" +
            "VALUES (:shardUuid, CURRENT_TIMESTAMP)")
    void insertDeletedShards(@Bind("shardUuid") Iterable<UUID> shardUuids);

    @Override
    // 'order by' is needed in this statement in order to make it compatible with statement-based replication
    @SqlUpdate("DELETE FROM transactions\n" +
            "WHERE end_time < :maxEndTime\n" +
            "  AND successful IN (TRUE, FALSE)\n" +
            "  AND transaction_id NOT IN (SELECT transaction_id FROM created_shards)\n" +
            "ORDER BY end_time, transaction_id\n" +
            "LIMIT " + CLEANUP_TRANSACTIONS_BATCH_SIZE)
    int deleteOldCompletedTransactions(Timestamp maxEndTime);
}
