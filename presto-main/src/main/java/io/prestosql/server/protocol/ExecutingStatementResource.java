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
package io.prestosql.server.protocol;

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.server.ForStatementResource;
import io.prestosql.server.protocol.Query.QueryResponse;
import io.prestosql.spi.QueryId;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SET_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/")
public class ExecutingStatementResource
{
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private static final DataSize DEFAULT_TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final DataSize MAX_TARGET_RESULT_SIZE = new DataSize(128, MEGABYTE);

    private final BoundedExecutor responseExecutor;
    private final QuerySubmissionManager querySubmissionManager;

    @Inject
    public ExecutingStatementResource(
            @ForStatementResource BoundedExecutor responseExecutor,
            QuerySubmissionManager querySubmissionManager)
    {
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.querySubmissionManager = requireNonNull(querySubmissionManager, "querySubmissionManager is null");
    }

    @GET
    @Path("/v1/statement/executing/{queryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @QueryParam("targetResultSize") DataSize targetResultSize,
            @HeaderParam(X_FORWARDED_PROTO) String proto,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug);
        if (isNullOrEmpty(proto)) {
            proto = uriInfo.getRequestUri().getScheme();
        }
        asyncQueryResults(query, token, maxWait, targetResultSize, uriInfo, proto, asyncResponse);
    }

    private void asyncQueryResults(
            Query query,
            long token,
            Duration maxWait,
            DataSize targetResultSize,
            UriInfo uriInfo,
            String scheme,
            AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        if (targetResultSize == null) {
            targetResultSize = DEFAULT_TARGET_RESULT_SIZE;
        }
        else {
            targetResultSize = Ordering.natural().min(targetResultSize, MAX_TARGET_RESULT_SIZE);
        }
        ListenableFuture<QueryResponse> queryResultsFuture = query.waitForResults(token, uriInfo, scheme, wait, targetResultSize);

        ListenableFuture<Response> response = Futures.transform(queryResultsFuture, ExecutingStatementResource::toResponse, directExecutor());

        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }

    private static Response toResponse(QueryResponse queryResponse)
    {
        ResponseBuilder response = Response.ok(queryResponse.getQueryResults());

        queryResponse.getSetCatalog().ifPresent(catalog -> response.header(PRESTO_SET_CATALOG, catalog));
        queryResponse.getSetSchema().ifPresent(schema -> response.header(PRESTO_SET_SCHEMA, schema));
        queryResponse.getSetPath().ifPresent(path -> response.header(PRESTO_SET_PATH, path));

        // add set session properties
        queryResponse.getSetSessionProperties()
                .forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

        // add clear session properties
        queryResponse.getResetSessionProperties()
                .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        // add set roles
        queryResponse.getSetRoles()
                .forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Entry<String, String> entry : queryResponse.getAddedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : queryResponse.getDeallocatedPreparedStatements()) {
            response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
        }

        // add new transaction ID
        queryResponse.getStartedTransactionId()
                .ifPresent(transactionId -> response.header(PRESTO_STARTED_TRANSACTION_ID, transactionId));

        // add clear transaction ID directive
        if (queryResponse.isClearTransactionId()) {
            response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
        }

        return response.build();
    }

    @DELETE
    @Path("/v1/statement/executing/{queryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        Query query = getQuery(queryId, slug);
        query.cancel();
        return Response.noContent().build();
    }

    private Query getQuery(QueryId queryId, String slug)
    {
        Optional<Query> query = querySubmissionManager.getQuery(queryId);
        if (query.map(q -> q.isSlugValid(slug)).orElse(false)) {
            return query.get();
        }
        throw badRequest(NOT_FOUND, "Query not found");
    }

    private static WebApplicationException badRequest(Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
