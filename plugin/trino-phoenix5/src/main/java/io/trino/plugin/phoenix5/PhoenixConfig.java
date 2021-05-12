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
package io.trino.plugin.phoenix5;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotNull;

import java.util.List;

public class PhoenixConfig
{
    private String connectionUrl;
    private List<String> resourceConfigFiles = ImmutableList.of();

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("phoenix.connection-url")
    public PhoenixConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public List<@FileExists String> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("phoenix.config.resources")
    public PhoenixConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(files);
        return this;
    }
}
