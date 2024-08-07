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
package io.trino.loki.model;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryResult
{
    @Test
    void testDeserializeStreams()
            throws IOException
    {
        final InputStream input =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("streams.json");
        QueryResult result = QueryResult.fromJSON(input);

        assertThat(result.getData().getResultType()).isEqualTo(Data.ResultType.Streams);
        assertThat(result.getData().getResult()).isInstanceOf(Streams.class);
        var streams = ((Streams) result.getData().getResult()).getStreams();
        assertThat(streams).hasSize(3);
        assertThat(streams.getFirst().values()).hasSize(89);
    }

    @Test
    void testDeserializeMatrix()
            throws IOException
    {
        final InputStream input =
                Thread.currentThread().getContextClassLoader().getResourceAsStream("matrix.json");
        QueryResult result = QueryResult.fromJSON(input);

        assertThat(result.getData().getResultType()).isEqualTo(Data.ResultType.Matrix);
        assertThat(result.getData().getResult()).isInstanceOf(Matrix.class);
        var metrics = ((Matrix) result.getData().getResult()).getMetrics();
        assertThat(metrics).hasSize(4);
        assertThat(metrics.getFirst().values()).hasSize(22);
    }
}
