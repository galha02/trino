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
package io.trino.plugin.iceberg;

import io.trino.testing.DistributedQueryRunner;

import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergMaterializedViewTest
        extends BaseIcebergMaterializedViewTest
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner();
    }

    @Override
    protected String getSchemaDirectory()
    {
        return getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("iceberg_data/tpch").toString();
    }
}
