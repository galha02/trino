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
package io.trino.cache;

import com.google.common.cache.Cache;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class CacheStatsMBean
{
    private final Cache<?, ?> cache;

    public CacheStatsMBean(Cache<?, ?> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Managed
    public long size()
    {
        return cache.size();
    }

    @Managed
    public Double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getLoadCount()
    {
        return cache.stats().loadCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }
}
