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
package io.trino.plugin.raptor.legacy.util;

import com.google.common.base.Throwables;
import com.mysql.cj.jdbc.JdbcStatement;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.reflect.Reflection.newProxy;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.ER_TRANS_CACHE_FULL;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_METADATA_ERROR;
import static java.sql.Types.INTEGER;
import static java.util.Objects.requireNonNull;

public final class DatabaseUtil
{
    private DatabaseUtil() {}

    public static <T> T onDemandDao(Jdbi dbi, Class<T> daoType)
    {
        requireNonNull(dbi, "dbi is null");
        return newProxy(daoType, (proxy, method, args) -> {
            try (Handle handle = dbi.open()) {
                T dao = handle.attach(daoType);
                return method.invoke(dao, args);
            }
            catch (JdbiException e) {
                throw metadataError(e);
            }
            catch (InvocationTargetException e) {
                throw metadataError(e.getCause());
            }
        });
    }

    public static <T> T runTransaction(Jdbi dbi, HandleCallback<T, RuntimeException> callback)
    {
        try {
            return dbi.inTransaction(callback);
        }
        catch (JdbiException e) {
            if (e.getCause() != null) {
                throwIfInstanceOf(e.getCause(), TrinoException.class);
            }
            throw metadataError(e);
        }
    }

    public static <T> void daoTransaction(Jdbi dbi, Class<T> daoType, Consumer<T> callback)
    {
        runTransaction(dbi, handle -> {
            callback.accept(handle.attach(daoType));
            return null;
        });
    }

    public static TrinoException metadataError(Throwable cause, String message)
    {
        return new TrinoException(RAPTOR_METADATA_ERROR, message, cause);
    }

    public static TrinoException metadataError(Throwable cause)
    {
        return metadataError(cause, "Failed to perform metadata operation");
    }

    /**
     * Run a SQL query as ignoring any constraint violations.
     * This allows idempotent inserts (equivalent to INSERT IGNORE).
     */
    public static void runIgnoringConstraintViolation(Runnable task)
    {
        try {
            task.run();
        }
        catch (RuntimeException e) {
            if (!sqlCodeStartsWith(e, "23")) {
                throw e;
            }
        }
    }

    public static void enableStreamingResults(Statement statement)
            throws SQLException
    {
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
    }

    public static OptionalInt getOptionalInt(ResultSet rs, String name)
            throws SQLException
    {
        int value = rs.getInt(name);
        return rs.wasNull() ? OptionalInt.empty() : OptionalInt.of(value);
    }

    public static OptionalLong getOptionalLong(ResultSet rs, String name)
            throws SQLException
    {
        long value = rs.getLong(name);
        return rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public static Long getBoxedLong(ResultSet rs, String name)
            throws SQLException
    {
        long value = rs.getLong(name);
        return rs.wasNull() ? null : value;
    }

    public static void bindOptionalInt(PreparedStatement statement, int index, OptionalInt value)
            throws SQLException
    {
        if (value.isPresent()) {
            statement.setInt(index, value.getAsInt());
        }
        else {
            statement.setNull(index, INTEGER);
        }
    }

    public static boolean isSyntaxOrAccessError(Exception e)
    {
        return sqlCodeStartsWith(e, "42");
    }

    public static boolean isTransactionCacheFullError(Exception e)
    {
        return mySqlErrorCodeMatches(e, ER_TRANS_CACHE_FULL);
    }

    /**
     * Check if an exception is caused by a MySQL exception of certain error code
     */
    private static boolean mySqlErrorCodeMatches(Exception e, int errorCode)
    {
        return Throwables.getCausalChain(e).stream()
                .filter(SQLException.class::isInstance)
                .map(SQLException.class::cast)
                .filter(t -> t.getErrorCode() == errorCode)
                .map(Throwable::getStackTrace)
                .anyMatch(isMySQLException());
    }

    private static Predicate<StackTraceElement[]> isMySQLException()
    {
        // check if the exception is a mysql exception by matching the package name in the stack trace
        return s -> Arrays.stream(s)
                .map(StackTraceElement::getClassName)
                .anyMatch(t -> t.startsWith("com.mysql.jdbc."));
    }

    private static boolean sqlCodeStartsWith(Exception e, String code)
    {
        for (Throwable throwable : Throwables.getCausalChain(e)) {
            if (throwable instanceof SQLException) {
                String state = ((SQLException) throwable).getSQLState();
                if (state != null && state.startsWith(code)) {
                    return true;
                }
            }
        }
        return false;
    }
}
