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
package io.trino.client;

import java.util.List;

/**
 * QueryData implementations are shared between the client and the server.
 * Implementing class is responsible for transferring result data over the wire (through @JsonProperty)
 * and deserializing incoming result data (through @JsonCreator) accordingly.
 * {@link JsonInlineQueryData} is an exception to the above as it is entirely handled by the {@link QueryDataJsonSerializationModule} .
 */
public interface QueryData
{
    Iterable<List<Object>> getData();

    boolean isPresent();

    default boolean isEmpty()
    {
        return !isPresent();
    }
}
