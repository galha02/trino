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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RenameColumn;
import org.testng.annotations.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRenameColumnTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testRenameColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT));

        getFutureValue(executeRenameColumn(asQualifiedName(tableName), identifier("a"), identifier("a_renamed"), false, false));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a_renamed", BIGINT), new ColumnMetadata("b", BIGINT));
    }

    @Test
    public void testRenameColumnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("not_existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameColumn(asQualifiedName(tableName), identifier("a"), identifier("a_renamed"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", tableName);
    }

    @Test
    public void testRenameColumnNotExistingTableIfExists()
    {
        QualifiedName tableName = qualifiedName("not_existing_table");

        getFutureValue(executeRenameColumn(tableName, identifier("a"), identifier("a_renamed"), true, false));
        // no exception
    }

    @Test
    public void testRenameMissingColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameColumn(asQualifiedName(tableName), identifier("missing_column"), identifier("test"), false, false)))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column 'missing_column' does not exist");
    }

    @Test
    public void testRenameColumnIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, simpleTable(tableName), false);
        TableHandle table = metadata.getTableHandle(testSession, tableName).get();

        getFutureValue(executeRenameColumn(asQualifiedName(tableName), identifier("missing_column"), identifier("test"), false, true));
        assertThat(metadata.getTableMetadata(testSession, table).getColumns())
                .containsExactly(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT));
    }

    @Test
    public void testRenameColumnOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameColumn(asQualifiedName(viewName), identifier("a"), identifier("a_renamed"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", viewName);
    }

    @Test
    public void testRenameColumnOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeRenameColumn(asQualifiedName(materializedViewName), identifier("a"), identifier("a_renamed"), false, false)))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist", materializedViewName);
    }

    private ListenableFuture<Void> executeRenameColumn(QualifiedName table, Identifier source, Identifier target, boolean tableExists, boolean columnExists)
    {
        return new RenameColumnTask(plannerContext.getMetadata(), new AllowAllAccessControl())
                .execute(new RenameColumn(new NodeLocation(1, 1), table, source, target, tableExists, columnExists), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private static ConnectorTableMetadata simpleTable(QualifiedObjectName tableName)
    {
        return new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.of(new ColumnMetadata("a", BIGINT), new ColumnMetadata("b", BIGINT)));
    }
}
