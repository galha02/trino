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
package io.trino.plugin.password.ldap;

import com.google.common.io.Closer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;

public class TestingOpenLdapServer
        extends TestingOpenLdapServerBase
        implements Closeable
{
    private final Closer closer = Closer.create();

    TestingOpenLdapServer(Network network)
    {
        super(network, "ghcr.io/trinodb/testing/centos7-oj17-openldap");
        closer.register(openLdapServer::close);
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
