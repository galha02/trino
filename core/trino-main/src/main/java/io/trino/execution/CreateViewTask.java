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
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.spi.security.Identity;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.sql.SqlFormatterUtil.getFormattedSql;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.CreateView.Security.INVOKER;
import static java.util.Objects.requireNonNull;

public class CreateViewTask
        implements DataDefinitionTask<CreateView>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final AnalyzerFactory analyzerFactory;

    @Inject
    public CreateViewTask(PlannerContext plannerContext, AccessControl accessControl, SqlParser sqlParser, AnalyzerFactory analyzerFactory)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
    }

    @Override
    public String getName()
    {
        return "CREATE VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateView statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Metadata metadata = plannerContext.getMetadata();
        if (!statement.getProperties().isEmpty()) {
            throw semanticException(NOT_SUPPORTED, statement, "Creating views with properties is not supported");
        }
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        accessControl.checkCanCreateView(session.toSecurityContext(), name);

        if (metadata.isMaterializedView(session, name)) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Materialized view already exists: '%s'", name);
        }
        if (metadata.isView(session, name)) {
            if (!statement.isReplace()) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "View already exists: '%s'", name);
            }
        }
        else if (metadata.getTableHandle(session, name).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Table already exists: '%s'", name);
        }

        String sql = getFormattedSql(statement.getQuery(), sqlParser);

        Analysis analysis = analyzerFactory.createAnalyzer(session, parameters, bindParameters(statement, parameters), stateMachine.getWarningCollector(), stateMachine.getPlanOptimizersStatsCollector())
                .analyze(statement);

        List<ViewColumn> columns = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ViewColumn(field.getName().get(), field.getType().getTypeId(), Optional.empty()))
                .collect(toImmutableList());

        // use DEFINER security by default
        Optional<Identity> owner = Optional.of(session.getIdentity());
        if (statement.getSecurity().orElse(null) == INVOKER) {
            owner = Optional.empty();
        }

        ViewDefinition definition = new ViewDefinition(
                sql,
                session.getCatalog(),
                session.getSchema(),
                columns,
                statement.getComment(),
                owner,
                session.getPath().getPath().stream()
                        // system path elements currently are not stored
                        .filter(element -> !element.getCatalogName().equals(GlobalSystemConnector.NAME))
                        .collect(toImmutableList()));

        metadata.createView(session, name, definition, statement.isReplace());

        stateMachine.setOutput(analysis.getTarget());
        stateMachine.setReferencedTables(analysis.getReferencedTables());

        return immediateVoidFuture();
    }
}
