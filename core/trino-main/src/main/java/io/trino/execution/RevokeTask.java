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
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.Privilege;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GrantOnType;
import io.trino.sql.tree.Revoke;

import javax.inject.Inject;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.INVALID_PRIVILEGE;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class RevokeTask
        implements DataDefinitionTask<Revoke>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public RevokeTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "REVOKE";
    }

    @Override
    public ListenableFuture<Void> execute(
            Revoke statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        if (statement.getType().filter(GrantOnType.SCHEMA::equals).isPresent()) {
            executeRevokeOnSchema(stateMachine.getSession(), statement);
        }
        else {
            executeRevokeOnTable(stateMachine.getSession(), statement);
        }
        return immediateVoidFuture();
    }

    private void executeRevokeOnSchema(Session session, Revoke statement)
    {
        CatalogSchemaName schemaName = createCatalogSchemaName(session, statement, Optional.of(statement.getName()));

        if (!metadata.schemaExists(session, schemaName)) {
            throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", schemaName);
        }

        Set<Privilege> privileges = parseStatementPrivileges(statement);
        for (Privilege privilege : privileges) {
            accessControl.checkCanRevokeSchemaPrivilege(session.toSecurityContext(), privilege, schemaName, createPrincipal(statement.getGrantee()), statement.isGrantOptionFor());
        }

        metadata.revokeSchemaPrivileges(session, schemaName, privileges, createPrincipal(statement.getGrantee()), statement.isGrantOptionFor());
    }

    private void executeRevokeOnTable(Session session, Revoke statement)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
        }

        Set<Privilege> privileges = parseStatementPrivileges(statement);
        for (Privilege privilege : privileges) {
            accessControl.checkCanRevokeTablePrivilege(session.toSecurityContext(), privilege, tableName, createPrincipal(statement.getGrantee()), statement.isGrantOptionFor());
        }

        metadata.revokeTablePrivileges(session, tableName, privileges, createPrincipal(statement.getGrantee()), statement.isGrantOptionFor());
    }

    private static Set<Privilege> parseStatementPrivileges(Revoke statement)
    {
        Set<Privilege> privileges;
        if (statement.getPrivileges().isPresent()) {
            privileges = statement.getPrivileges().get().stream()
                    .map(privilege -> parsePrivilege(statement, privilege))
                    .collect(toImmutableSet());
        }
        else {
            // All privileges
            privileges = EnumSet.allOf(Privilege.class);
        }
        return privileges;
    }

    private static Privilege parsePrivilege(Revoke statement, String privilegeString)
    {
        for (Privilege privilege : Privilege.values()) {
            if (privilege.name().equalsIgnoreCase(privilegeString)) {
                return privilege;
            }
        }

        throw semanticException(INVALID_PRIVILEGE, statement, "Unknown privilege: '%s'", privilegeString);
    }
}
