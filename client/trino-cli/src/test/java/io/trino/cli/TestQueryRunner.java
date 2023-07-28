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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.JsonQueryResults;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.uri.PropertyName;
import io.trino.client.uri.TrinoUri;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;

import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.cli.ClientOptions.OutputFormat.CSV;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.auth.external.ExternalRedirectStrategy.PRINT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestQueryRunner
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private MockWebServer server;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
        server = null;
    }

    @Test
    public void testCookie()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("/v1/statement"))
                .addHeader(SET_COOKIE, "a=apple"));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        QueryRunner queryRunner = createQueryRunner(createTrinoUri(server, false), createClientSession(server));

        try (Query query = queryRunner.startQuery("first query will introduce a cookie")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
        }
        try (Query query = queryRunner.startQuery("second query should carry the cookie")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
        }

        assertNull(server.takeRequest().getHeader("Cookie"));
        assertEquals(server.takeRequest().getHeader("Cookie"), "a=apple");
        assertEquals(server.takeRequest().getHeader("Cookie"), "a=apple");
    }

    static TrinoUri createTrinoUri(MockWebServer server, boolean insecureSsl)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.toString(), PRINT.name());
        properties.setProperty(PropertyName.SSL.toString(), Boolean.toString(!insecureSsl));
        return TrinoUri.create(server.url("/").uri(), properties);
    }

    static ClientSession createClientSession(MockWebServer server)
    {
        return ClientSession.builder()
                .server(server.url("/").uri())
                .principal(Optional.of("user"))
                .source("source")
                .clientInfo("clientInfo")
                .catalog("catalog")
                .schema("schema")
                .timeZone(ZoneId.of("America/Los_Angeles"))
                .locale(Locale.ENGLISH)
                .transactionId(null)
                .clientRequestTimeout(new Duration(2, MINUTES))
                .compressionDisabled(true)
                .build();
    }

    static String createResults(MockWebServer server)
    {
        QueryResults queryResults = new JsonQueryResults(QueryResults.builder()
                .withQueryId("20160128_214710_00012_rk68b")
                .withInfoUri(server.url("/query.html?20160128_214710_00012_rk68b").uri())
                .withColumns(ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))))
                .withStats(StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build())
                .buildEmpty(), ImmutableList.of(ImmutableList.of(123)));
        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    static QueryRunner createQueryRunner(TrinoUri uri, ClientSession clientSession)
    {
        return new QueryRunner(
                uri,
                clientSession,
                false,
                HttpLoggingInterceptor.Level.NONE);
    }

    static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
