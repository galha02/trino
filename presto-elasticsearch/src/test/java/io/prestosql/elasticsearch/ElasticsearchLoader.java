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
package io.prestosql.elasticsearch;

import io.prestosql.Session;
import io.prestosql.client.Column;
import io.prestosql.client.QueryData;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.type.Type;
import io.prestosql.tests.AbstractTestingPrestoClient;
import io.prestosql.tests.ResultsSession;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

public class ElasticsearchLoader
        extends AbstractTestingPrestoClient<Void>
{
    private final String tableName;
    private final Client client;

    public ElasticsearchLoader(
            Client client,
            String tableName,
            TestingPrestoServer prestoServer,
            Session defaultSession)
    {
        super(prestoServer, defaultSession);

        this.tableName = requireNonNull(tableName, "tableName is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new ElasticsearchLoadingSession();
    }

    private class ElasticsearchLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private ElasticsearchLoadingSession() {}

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() == null) {
                return;
            }
            checkState(types.get() != null, "Type information is missing");
            List<Column> columns = statusInfo.getColumns();
            for (List<Object> fields : data.getData()) {
                try {
                    XContentBuilder dataBuilder = jsonBuilder().startObject();
                    for (int i = 0; i < fields.size(); i++) {
                        Type type = types.get().get(i);
                        Object value = convertValue(fields.get(i), type);
                        dataBuilder.field(columns.get(i).getName(), value);
                    }
                    dataBuilder.endObject();
                    client.prepareIndex(tableName, "doc")
                            .setSource(dataBuilder.string(), JSON)
                            .get();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error loading data into Elasticsearch index: " + tableName, e);
                }
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (type == BOOLEAN || type == DATE || isVarcharType(type)) {
                return value;
            }
            if (type == BIGINT) {
                return ((Number) value).longValue();
            }
            if (type == INTEGER) {
                return ((Number) value).intValue();
            }
            if (type == DOUBLE) {
                return ((Number) value).doubleValue();
            }
            throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }
}
