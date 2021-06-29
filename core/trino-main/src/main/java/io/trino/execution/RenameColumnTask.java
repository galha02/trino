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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RenameColumn;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;

public class RenameColumnTask
        implements DataDefinitionTask<RenameColumn>
{
    @Override
    public String getName()
    {
        return "RENAME COLUMN";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameColumn statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        Optional<TableHandle> tableHandleOptional = metadata.getTableHandle(session, tableName);
        if (tableHandleOptional.isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = tableHandleOptional.get();

        String source = statement.getSource().getValue().toLowerCase(ENGLISH);
        String target = statement.getTarget().getValue().toLowerCase(ENGLISH);

        accessControl.checkCanRenameColumn(session.toSecurityContext(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle columnHandle = columnHandles.get(source);
        if (columnHandle == null) {
            if (!statement.isColumnExists()) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", source);
            }
            return immediateVoidFuture();
        }

        if (columnHandles.containsKey(target)) {
            throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", target);
        }

        if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot rename hidden column");
        }

        metadata.renameColumn(session, tableHandle, columnHandle, target);

        return immediateVoidFuture();
    }
}
