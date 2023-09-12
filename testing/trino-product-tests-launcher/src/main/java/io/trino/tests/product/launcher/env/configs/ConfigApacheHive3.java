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
package io.trino.tests.product.launcher.env.configs;

public class ConfigApacheHive3
        extends ConfigDefault
{
    @Override
    public String getHadoopBaseImage()
    {
        return "ghcr.io/trinodb/testing/hive3.1-hive";
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hms-only.yaml";
    }
}
