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
package io.trino.server.protocol.data;

import io.trino.Session;
import io.trino.spi.QueryId;
import jakarta.ws.rs.core.UriInfo;

import java.util.Set;

public class DefaultQueryDataProducerFactory
        implements QueryDataProducerFactory
{
    @Override
    public QueryDataProducer create(Session session, QueryId queryId, Set<String> supportedFormats, UriInfo uriInfo)
    {
        // Always return data as inlined JSON-serialized array of arrays of values
        return new InlineJsonQueryDataProducer();
    }
}
