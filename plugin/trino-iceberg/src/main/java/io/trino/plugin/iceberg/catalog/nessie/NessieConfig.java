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
package io.trino.plugin.iceberg.catalog.nessie;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotEmpty;

public class NessieConfig
{
    private String defaultReferenceName = "main";
    private String serverUri;
    private String defaultWarehouseDir;

    public String getDefaultReferenceName()
    {
        return defaultReferenceName;
    }

    @Config("iceberg.nessie.ref")
    @ConfigDescription("The default Nessie reference to work on")
    public NessieConfig setDefaultReferenceName(String defaultReferenceName)
    {
        this.defaultReferenceName = defaultReferenceName;
        return this;
    }

    public String getServerUri()
    {
        return serverUri;
    }

    @Config("iceberg.nessie.uri")
    @ConfigDescription("The URI to connect to the Nessie server")
    public NessieConfig setServerUri(String serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @NotEmpty
    public String getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("iceberg.nessie.default-warehouse-dir")
    @ConfigDescription("The default warehouse to use for Nessie")
    public NessieConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = defaultWarehouseDir;
        return this;
    }
}
