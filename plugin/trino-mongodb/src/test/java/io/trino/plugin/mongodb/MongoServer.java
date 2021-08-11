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
package io.trino.plugin.mongodb;

import com.google.common.net.HostAndPort;
import com.mongodb.MongoCredential;
import org.testcontainers.containers.MongoDBContainer;

import java.io.Closeable;

public class MongoServer
        implements Closeable
{
    private static final int MONGO_PORT = 27017;

    private final MongoDBContainer dockerContainer;

    private static final String USER = "mongoAdmin";
    private static final String PASSWORD = "secret1234";
    private static final String DATABASE = "tpch";

    public MongoServer()
    {
        this("4.0.0");
    }

    public MongoServer(String mongoVersion)
    {
        this.dockerContainer = new MongoDBContainer("mongo:" + mongoVersion)
                .withStartupAttempts(3)
                .withCommand("--bind_ip 0.0.0.0");
        this.dockerContainer.start();
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(MONGO_PORT));
    }

    public MongoCredential getDefaultCredentials()
    {
        if (DATABASE.equals("tpch")) {
            return null;
        }

        return MongoCredential.createCredential(USER, DATABASE, PASSWORD.toCharArray());
    }

    public String getCredentialString()
    {
        if (DATABASE.equals("tpch")) {
            return "";
        }

        return String.format("%s:%s@%s", USER, PASSWORD, DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
