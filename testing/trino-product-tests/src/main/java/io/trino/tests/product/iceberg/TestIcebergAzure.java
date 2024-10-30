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
package io.trino.tests.product.iceberg;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tests.product.BaseTestTableFormats;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.ICEBERG_AZURE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestIcebergAzure
        extends BaseTestTableFormats
{
    @Inject
    @Named("databases.trino.abfs_schema")
    private String schema;
    private String schemaLocation;

    @BeforeMethodWithContext
    public void setUp()
    {
        String container = requireNonNull(System.getenv("ABFS_CONTAINER"), "Environment variable not set: ABFS_CONTAINER");
        String account = requireNonNull(System.getenv("ABFS_ACCOUNT"), "Environment variable not set: ABFS_ACCOUNT");
        schemaLocation = format("abfs://%s@%s.dfs.core.windows.net/%s", container, account, schema);
    }

    @Override
    protected String getCatalogName()
    {
        return "iceberg";
    }

    @Test(groups = {ICEBERG_AZURE, PROFILE_SPECIFIC_TESTS})
    public void testCreateAndSelectNationTable()
    {
        super.testCreateAndSelectNationTable(schemaLocation);
    }

    @Test(groups = {ICEBERG_AZURE, PROFILE_SPECIFIC_TESTS})
    public void testBasicWriteOperations()
    {
        super.testBasicWriteOperations(schemaLocation);
    }

    @Test(groups = {ICEBERG_AZURE, PROFILE_SPECIFIC_TESTS})
    public void testPathContainsSpecialCharacter()
    {
        super.testPathContainsSpecialCharacter(schemaLocation, "partitioning");
    }

    @Test(groups = {ICEBERG_AZURE, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingTrinoData()
    {
        super.testSparkCompatibilityOnTrinoCreatedTable(schemaLocation);
    }

    @Override
    protected String getSparkCatalog()
    {
        return "iceberg_test";
    }
}
