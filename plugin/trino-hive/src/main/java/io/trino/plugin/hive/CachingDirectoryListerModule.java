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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CachingDirectoryListerModule
        implements Module
{
    private final Optional<CachingDirectoryLister> cachingDirectoryLister;

    public CachingDirectoryListerModule(Optional<CachingDirectoryLister> cachingDirectoryLister)
    {
        this.cachingDirectoryLister = requireNonNull(cachingDirectoryLister, "cachingDirectoryLister is null");
    }

    @Override
    public void configure(Binder binder)
    {
        if (cachingDirectoryLister.isPresent()) {
            CachingDirectoryLister directoryLister = cachingDirectoryLister.get();
            binder.bind(DirectoryLister.class).toInstance(directoryLister);
            binder.bind(TableInvalidationCallback.class).toInstance(directoryLister);
        }
        else {
            binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
            binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class);
            binder.bind(TableInvalidationCallback.class).to(CachingDirectoryLister.class);
        }
    }
}
