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
package io.prestosql.tests.product.launcher.env;

import java.io.File;

import static picocli.CommandLine.Option;

public final class EnvironmentOptions
{
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

    @Option(names = "--config", paramLabel = "<config>", description = "Environment config to use")
    public String config = "config-default";

    @Option(names = "--server-package", paramLabel = "<package>", description = "Path to Presto server package " + DEFAULT_VALUE, defaultValue = "${server.module}/target/${server.name}-${project.version}.tar.gz")
    public File serverPackage;

    @Option(names = "--without-presto", description = "Do not start presto-master")
    public boolean withoutPrestoMaster;

    @Option(names = "--no-bind", description = "Bind ports on localhost", negatable = true)
    public boolean bindPorts = true;

    @Option(names = "--debug", description = "Open Java debug ports")
    public boolean debug;

    public static EnvironmentOptions empty()
    {
        return new EnvironmentOptions();
    }
}
