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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

final class TestingDatabase
        implements AutoCloseable
{
    private static final ConnectorSession session = testSessionBuilder().build().toConnectorSession();

    private final Connection connection;
    private final JdbcClient jdbcClient;

    public TestingDatabase()
            throws SQLException
    {
        String connectionUrl = "jdbc:h2:mem:test" + System.nanoTime();
        jdbcClient = new BaseJdbcClient(
                new BaseJdbcConfig(),
                "\"",
                new DriverConnectionFactory(new Driver(), connectionUrl, new Properties()));

        connection = DriverManager.getConnection(connectionUrl);
        connection.createStatement().execute("CREATE SCHEMA example");

        connection.createStatement().execute("CREATE TABLE example.numbers(text varchar primary key, text_short varchar(32), value bigint)");
        connection.createStatement().execute("INSERT INTO example.numbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");
        connection.createStatement().execute("CREATE TABLE example.view_source(id varchar primary key)");
        connection.createStatement().execute("CREATE VIEW example.view AS SELECT id FROM example.view_source");
        connection.createStatement().execute("CREATE SCHEMA tpch");
        connection.createStatement().execute("CREATE TABLE tpch.orders(orderkey bigint primary key, custkey bigint)");
        connection.createStatement().execute("CREATE TABLE tpch.lineitem(orderkey bigint primary key, partkey bigint)");

        connection.createStatement().execute("CREATE SCHEMA exa_ple");
        connection.createStatement().execute("CREATE TABLE exa_ple.num_ers(te_t varchar primary key, \"VA%UE\" bigint)");
        connection.createStatement().execute("CREATE TABLE exa_ple.table_with_float_col(col1 bigint, col2 double, col3 float, col4 real)");

        connection.commit();
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public JdbcSplit getSplit(String schemaName, String tableName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        SchemaTableName table = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle jdbcTableHandle = jdbcClient.getTableHandle(identity, table)
                .orElseThrow(() -> new IllegalArgumentException("table not found: " + table));
        JdbcTableLayoutHandle jdbcLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.all());
        ConnectorSplitSource splits = jdbcClient.getSplits(identity, jdbcLayoutHandle);
        return (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
    }

    public Map<String, JdbcColumnHandle> getColumnHandles(String schemaName, String tableName)
    {
        Optional<JdbcTableHandle> tableHandle = jdbcClient.getTableHandle(JdbcIdentity.from(session), new SchemaTableName(schemaName, tableName));
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(session, tableHandle.orElse(null));
        ImmutableMap.Builder<String, JdbcColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : columns) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }
}
