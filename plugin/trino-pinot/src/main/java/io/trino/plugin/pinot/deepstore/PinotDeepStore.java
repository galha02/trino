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
package io.trino.plugin.pinot.deepstore;

import com.google.inject.Inject;
import org.apache.pinot.spi.env.PinotConfiguration;

import java.net.URI;

public class PinotDeepStore
        implements DeepStore
{
    private final URI deepStoreUri;
    private final PinotConfiguration pinotConfiguration;

    public enum DeepStoreProvider
    {
        NONE,
        GCS,
        S3,
    }

    @Inject
    public PinotDeepStore(PinotConfiguration pinotConfiguration, PinotDeepStoreConfig pinotDeepStoreConfig)
    {
        this.deepStoreUri = pinotDeepStoreConfig.getDeepStoreUri().orElseThrow(() -> new IllegalStateException("pinotDeepStore URI not present "));
        this.pinotConfiguration = pinotConfiguration.subset("storage.factory");
    }

    @Override
    public URI getDeepStoreUri()
    {
        return deepStoreUri;
    }

    @Override
    public PinotConfiguration getPinotConfiguration()
    {
        return pinotConfiguration;
    }
}
