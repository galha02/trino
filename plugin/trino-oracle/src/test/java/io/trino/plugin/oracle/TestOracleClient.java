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
package io.trino.plugin.oracle;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import oracle.jdbc.OracleTypes;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleClient
{
    private static final JdbcClient CLIENT = new OracleClient(
            new BaseJdbcConfig(),
            new OracleConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    @Test
    public void testTypedNullWriteMapping()
            throws SQLException
    {
        testTypedNullWriteMapping(BOOLEAN, "?", Types.TINYINT);
        testTypedNullWriteMapping(TINYINT, "?", Types.TINYINT);
        testTypedNullWriteMapping(SMALLINT, "?", Types.SMALLINT);
        testTypedNullWriteMapping(INTEGER, "?", Types.INTEGER);
        testTypedNullWriteMapping(BIGINT, "?", Types.BIGINT);
        testTypedNullWriteMapping(REAL, "?", Types.REAL);
        testTypedNullWriteMapping(DOUBLE, "?", Types.DOUBLE);
        testTypedNullWriteMapping(VARBINARY, "?", Types.VARBINARY);
        testTypedNullWriteMapping(createCharType(25), "?", Types.NCHAR);
        testTypedNullWriteMapping(createDecimalType(16, 6), "?", Types.DECIMAL);
        testTypedNullWriteMapping(createDecimalType(36, 12), "?", Types.DECIMAL);
        testTypedNullWriteMapping(createUnboundedVarcharType(), "?", Types.VARCHAR);
        testTypedNullWriteMapping(createVarcharType(123), "?", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_SECONDS, "TO_DATE(?, 'SYYYY-MM-DD HH24:MI:SS')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_MILLIS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF3')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_MICROS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF6')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_NANOS, "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF9')", Types.VARCHAR);
        testTypedNullWriteMapping(TIMESTAMP_TZ_MILLIS, "?", OracleTypes.TIMESTAMPTZ);
        testTypedNullWriteMapping(DATE, "TO_DATE(?, 'SYYYY-MM-DD')", Types.VARCHAR);
    }

    private void testTypedNullWriteMapping(Type type, String bindExpression, int nullJdbcType)
            throws SQLException
    {
        WriteMapping writeMapping = CLIENT.toWriteMapping(SESSION, type);
        assertThat(writeMapping.getWriteFunction()).isNotNull();
        WriteFunction writeFunction = writeMapping.getWriteFunction();

        assertThat(writeFunction.getBindExpression()).isEqualTo(bindExpression);

        PreparedStatement statementProxy = newProxy(PreparedStatement.class, (proxy, method, args) -> {
            // Calling setNull with a proper JDBC type is important for Oracle as it
            // allows prepared statement to be cached and reused by the database cursor
            // while writing. If the default implementation of the WriteFunction is used,
            // NULL is sent with the JDBC type 0, which invalidates the cache for prepared statement.
            // After invalidation, statement needs to be parsed and analyzed again,
            // which has severe performance cost when writing large datasets containing NULLs.
            assertThat(method.getName()).isEqualTo("setNull");
            assertThat(args.length).isEqualTo(2);
            assertThat(args[0]).isEqualTo(1325);
            assertThat(args[1])
                    .describedAs("expected jdbc type for NULL value")
                    .isEqualTo(nullJdbcType);

            return null;
        });

        writeFunction.setNull(statementProxy, 1325);
    }
}
