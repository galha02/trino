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
package io.trino.filesystem;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class UriLocation
{
    private final URI uri;
    private final Map<String, List<String>> headers;

    public UriLocation(URI uri, Map<String, List<String>> headers)
    {
        this.uri = requireNonNull(uri, "uri is null");
        this.headers = ImmutableMap.copyOf(requireNonNull(headers, "headers is null"));
    }

    public URI uri()
    {
        return uri;
    }

    public Map<String, List<String>> headers()
    {
        return headers;
    }
}
