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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FailureAwareThriftMetastoreClient
        implements ThriftMetastoreClient
{
    public interface Callback
    {
        void success();

        void failed(TException e);
    }

    private final ThriftMetastoreClient delegate;
    private final Callback callback;

    public FailureAwareThriftMetastoreClient(ThriftMetastoreClient client, Callback callback)
    {
        this.delegate = requireNonNull(client, "client is null");
        this.callback = requireNonNull(callback, "callback is null");
    }

    @VisibleForTesting
    public ThriftMetastoreClient getDelegate()
    {
        return delegate;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        return runWithHandle(delegate::getAllDatabases);
    }

    @Override
    public Database getDatabase(String databaseName)
            throws TException
    {
        return runWithHandle(() -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws TException
    {
        return runWithHandle(() -> delegate.getAllTables(databaseName));
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableNamesByFilter(databaseName, filter));
    }

    @Override
    public List<String> getTableNamesByType(String databaseName, String tableType)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableNamesByType(databaseName, tableType));
    }

    @Override
    public void createDatabase(Database database)
            throws TException
    {
        runWithHandle(() -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        runWithHandle(() -> delegate.dropDatabase(databaseName, deleteData, cascade));
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        runWithHandle(() -> delegate.alterDatabase(databaseName, database));
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        runWithHandle(() -> delegate.createTable(table));
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        runWithHandle(() -> delegate.dropTable(databaseName, name, deleteData));
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
            throws TException
    {
        runWithHandle(() -> delegate.alterTable(databaseName, tableName, newTable));
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Table getTableWithCapabilities(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableWithCapabilities(databaseName, tableName));
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getFields(databaseName, tableName));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        runWithHandle(() -> delegate.setTableColumnStatistics(databaseName, tableName, statistics));
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        runWithHandle(() -> delegate.deleteTableColumnStatistics(databaseName, tableName, columnName));
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames));
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        runWithHandle(() -> delegate.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics));
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        runWithHandle(() -> delegate.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName));
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionNames(databaseName, tableName));
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionNamesFiltered(databaseName, tableName, partitionValues));
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        return runWithHandle(() -> delegate.addPartitions(newPartitions));
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return runWithHandle(() -> delegate.dropPartition(databaseName, tableName, partitionValues, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        runWithHandle(() -> delegate.alterPartition(databaseName, tableName, partition));
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartition(databaseName, tableName, partitionValues));
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionsByNames(databaseName, tableName, partitionNames));
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return runWithHandle(() -> delegate.listRoles(principalName, principalType));
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException
    {
        return runWithHandle(() -> delegate.listPrivileges(principalName, principalType, hiveObjectRef));
    }

    @Override
    public List<String> getRoleNames()
            throws TException
    {
        return runWithHandle(delegate::getRoleNames);
    }

    @Override
    public void createRole(String role, String grantor)
            throws TException
    {
        runWithHandle(() -> delegate.createRole(role, grantor));
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        runWithHandle(() -> delegate.dropRole(role));
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return runWithHandle(() -> delegate.grantPrivileges(privilegeBag));
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return runWithHandle(() -> delegate.revokePrivileges(privilegeBag));
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        runWithHandle(() -> delegate.grantRole(role, granteeName, granteeType, grantorName, grantorType, grantOption));
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        runWithHandle(() -> delegate.revokeRole(role, granteeName, granteeType, grantOption));
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
            throws TException
    {
        return runWithHandle(() -> delegate.listRoleGrants(name, principalType));
    }

    @Override
    public void setUGI(String userName)
            throws TException
    {
        runWithHandle(() -> delegate.setUGI(userName));
    }

    @Override
    public long openTransaction(String user)
            throws TException
    {
        return runWithHandle(() -> delegate.openTransaction(user));
    }

    @Override
    public void commitTransaction(long transactionId)
            throws TException
    {
        runWithHandle(() -> delegate.commitTransaction(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
            throws TException
    {
        runWithHandle(() -> delegate.sendTransactionHeartbeat(transactionId));
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
            throws TException
    {
        return runWithHandle(() -> delegate.acquireLock(lockRequest));
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws TException
    {
        return runWithHandle(() -> delegate.checkLock(lockId));
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId)
            throws TException
    {
        return runWithHandle(() -> delegate.getValidWriteIds(tableList, currentTransactionId));
    }

    @Override
    public String get_config_value(String name, String defaultValue)
            throws TException
    {
        return runWithHandle(() -> delegate.get_config_value(name, defaultValue));
    }

    @Override
    public String getDelegationToken(String userName)
            throws TException
    {
        return runWithHandle(() -> delegate.getDelegationToken(userName));
    }

    private <T> T runWithHandle(ThrowingSupplier<T> supplier)
            throws TException
    {
        try {
            T result = supplier.get();
            callback.success();
            return result;
        }
        catch (TException thriftException) {
            try {
                callback.failed(thriftException);
            }
            catch (RuntimeException callbackException) {
                callbackException.addSuppressed(thriftException);
                throw callbackException;
            }
            throw thriftException;
        }
    }

    private void runWithHandle(ThrowingRunnable runnable)
            throws TException
    {
        try {
            runnable.run();
            callback.success();
        }
        catch (TException thriftException) {
            try {
                callback.failed(thriftException);
            }
            catch (RuntimeException callbackException) {
                callbackException.addSuppressed(thriftException);
                throw callbackException;
            }
            throw thriftException;
        }
    }

    private interface ThrowingSupplier<T>
    {
        T get()
                throws TException;
    }

    private interface ThrowingRunnable
    {
        void run()
                throws TException;
    }
}
