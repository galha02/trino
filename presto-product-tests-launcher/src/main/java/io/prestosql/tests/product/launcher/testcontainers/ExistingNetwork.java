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
package io.prestosql.tests.product.launcher.testcontainers;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.Network;

import static java.util.Objects.requireNonNull;

public final class ExistingNetwork
        implements Network
{
    private final String networkId;

    public ExistingNetwork(String networkId)
    {
        this.networkId = requireNonNull(networkId, "networkId is null");
    }

    @Override
    public String getId()
    {
        return networkId;
    }

    @Override
    public void close() {}

    @Override
    public Statement apply(Statement statement, Description description)
    {
        // junit4 integration
        throw new UnsupportedOperationException();
    }
}
