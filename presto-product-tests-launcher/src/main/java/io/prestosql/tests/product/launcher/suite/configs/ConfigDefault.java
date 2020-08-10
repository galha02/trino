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
package io.prestosql.tests.product.launcher.suite.configs;

import io.prestosql.tests.product.launcher.env.EnvironmentDefaults;
import io.prestosql.tests.product.launcher.suite.SuiteConfig;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.tests.product.launcher.suite.Suites.nameForConfigClass;

public class ConfigDefault
        implements SuiteConfig
{
    @Override
    public String getHadoopBaseImage()
    {
        return EnvironmentDefaults.getHadoopBaseImage();
    }

    @Override
    public String getImagesVersion()
    {
        return EnvironmentDefaults.getImagesVersion();
    }

    @Override
    public String getHadoopImagesVersion()
    {
        return EnvironmentDefaults.getHadoopImagesVersion();
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return EnvironmentDefaults.getTemptoConfigurationFile();
    }

    @Override
    public String getConfigName()
    {
        return nameForConfigClass(getClass());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hadoopBaseImage", getHadoopBaseImage())
                .add("hadoopImagesVersion", getHadoopImagesVersion())
                .add("hadoopInitScript", getHadoopInitScript())
                .add("imagesVersion", getImagesVersion())
                .add("excludedGroups", getExcludedGroups())
                .add("excludedTests", getExcludedTests())
                .add("temptoEnvironmentConfigFile", getTemptoEnvironmentConfigFile())
                .toString();
    }
}
