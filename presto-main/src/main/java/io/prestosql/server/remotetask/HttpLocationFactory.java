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
package io.prestosql.server.remotetask;

import io.airlift.http.server.HttpServerInfo;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.spi.QueryId;

import javax.inject.Inject;

import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class HttpLocationFactory
        implements LocationFactory
{
    private final InternalNodeManager nodeManager;
    private final URI baseUri;

    @Inject
    public HttpLocationFactory(InternalNodeManager nodeManager, HttpServerInfo httpServerInfo, InternalCommunicationConfig config)
    {
        this(nodeManager, config.isHttpsRequired() ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri());
    }

    public HttpLocationFactory(InternalNodeManager nodeManager, URI baseUri)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.baseUri = requireNonNull(baseUri, "baseUri is null");
    }

    @Override
    public URI createQueryLocation(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("/v1/query")
                .appendPath(queryId.toString())
                .build();
    }

    @Override
    public URI createStageLocation(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");
        return uriBuilderFrom(baseUri)
                .appendPath("v1/stage")
                .appendPath(stageId.toString())
                .build();
    }

    @Override
    public URI createLocalTaskLocation(TaskId taskId)
    {
        return createTaskLocation(nodeManager.getCurrentNode(), taskId);
    }

    @Override
    public URI createTaskLocation(InternalNode node, TaskId taskId)
    {
        requireNonNull(node, "node is null");
        requireNonNull(taskId, "taskId is null");
        return uriBuilderFrom(node.getInternalUri())
                .appendPath("/v1/task")
                .appendPath(taskId.toString())
                .build();
    }

    @Override
    public URI createMemoryInfoLocation(InternalNode node)
    {
        requireNonNull(node, "node is null");
        return uriBuilderFrom(node.getInternalUri())
                .appendPath("/v1/memory").build();
    }
}
