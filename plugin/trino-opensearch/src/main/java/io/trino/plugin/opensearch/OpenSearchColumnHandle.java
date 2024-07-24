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
package io.trino.plugin.opensearch;

import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record OpenSearchColumnHandle(
        String name,
        Type type,
        IndexMetadata.Type opensearchType,
        DecoderDescriptor decoderDescriptor,
        boolean supportsPredicates)
        implements ColumnHandle
{
    public OpenSearchColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(opensearchType, "opensearchType is null");
        requireNonNull(decoderDescriptor, "decoderDescriptor is null");
    }

    @Override
    public String toString()
    {
        return name() + "::" + type();
    }
}
