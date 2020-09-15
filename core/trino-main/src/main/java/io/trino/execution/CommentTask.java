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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.MISSING_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class CommentTask
        implements DataDefinitionTask<Comment>
{
    @Override
    public String getName()
    {
        return "COMMENT";
    }

    @Override
    public ListenableFuture<?> execute(
            Comment statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        if (statement.getType() == Comment.Type.TABLE) {
            QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
            if (tableHandle.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", tableName);
            }

            accessControl.checkCanSetTableComment(session.toSecurityContext(), tableName);

            metadata.setTableComment(session, tableHandle.get(), statement.getComment());
        }
        else if (statement.getType() == Comment.Type.COLUMN) {
            Optional<QualifiedName> prefix = statement.getName().getPrefix();
            if (prefix.isEmpty()) {
                throw semanticException(MISSING_TABLE, statement, "Table must be specified");
            }

            QualifiedObjectName tableName = createQualifiedObjectName(session, statement, prefix.get());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
            if (tableHandle.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: " + tableName);
            }

            String columnName = statement.getName().getSuffix();
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());
            if (!columnHandles.containsKey(columnName)) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column does not exist: " + columnName);
            }

            accessControl.checkCanSetColumnComment(session.toSecurityContext(), tableName);

            metadata.setColumnComment(session, tableHandle.get(), columnHandles.get(columnName), statement.getComment());
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported comment type: %s", statement.getType());
        }

        return immediateFuture(null);
    }
}
