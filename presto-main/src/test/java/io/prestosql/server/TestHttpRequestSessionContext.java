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
package io.prestosql.server;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static io.prestosql.client.PrestoHeaders.PRESTO_LANGUAGE;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_ROLE;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static org.testng.Assert.assertEquals;

public class TestHttpRequestSessionContext
{
    @Test
    public void testSessionContext()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_PATH, "testPath")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "client-info")
                        .put(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                        .put(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=select * from foo,query2=select * from bar")
                        .put(PRESTO_ROLE, "foo_connector=ALL")
                        .put(PRESTO_ROLE, "bar_connector=NONE")
                        .put(PRESTO_ROLE, "foobar_connector=ROLE{role}")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.foo=bar")
                        .put(PRESTO_EXTRA_CREDENTIAL, "test.token.abc=xyz")
                        .build(),
                "testRemote");

        HttpRequestSessionContext context = new HttpRequestSessionContext(request);
        assertEquals(context.getSource(), "testSource");
        assertEquals(context.getCatalog(), "testCatalog");
        assertEquals(context.getSchema(), "testSchema");
        assertEquals(context.getPath(), "testPath");
        assertEquals(context.getIdentity(), new Identity("testUser", Optional.empty()));
        assertEquals(context.getClientInfo(), "client-info");
        assertEquals(context.getLanguage(), "zh-TW");
        assertEquals(context.getTimeZoneId(), "Asia/Taipei");
        assertEquals(context.getSystemProperties(), ImmutableMap.of(QUERY_MAX_MEMORY, "1GB", JOIN_DISTRIBUTION_TYPE, "partitioned", HASH_PARTITION_COUNT, "43"));
        assertEquals(context.getPreparedStatements(), ImmutableMap.of("query1", "select * from foo", "query2", "select * from bar"));
        assertEquals(context.getIdentity().getRoles(), ImmutableMap.of(
                "foo_connector", new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                "bar_connector", new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                "foobar_connector", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role"))));
        assertEquals(context.getIdentity().getExtraCredentials(), ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz"));
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testPreparedStatementsHeaderDoesNotParse()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, "testUser")
                        .put(PRESTO_SOURCE, "testSource")
                        .put(PRESTO_CATALOG, "testCatalog")
                        .put(PRESTO_SCHEMA, "testSchema")
                        .put(PRESTO_PATH, "testPath")
                        .put(PRESTO_LANGUAGE, "zh-TW")
                        .put(PRESTO_TIME_ZONE, "Asia/Taipei")
                        .put(PRESTO_CLIENT_INFO, "null")
                        .put(PRESTO_PREPARED_STATEMENT, "query1=abcdefg")
                        .build(),
                "testRemote");
        new HttpRequestSessionContext(request);
    }
}
