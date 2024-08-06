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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.Lists;

import java.io.IOException;

public class MatrixDeserializer
        extends JsonDeserializer<Matrix>
{
    @Override
    public Matrix deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        Matrix matrix = new Matrix();
        if (jp.currentToken() == JsonToken.START_ARRAY) {
            jp.nextToken();
            matrix.setMetrics(Lists.newArrayList(jp.readValuesAs(Matrix.Metric.class)));
        }

        return matrix;
    }
}
