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
package io.trino.connector;

import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkArgument;

public interface CatalogServiceProvider<T>
{
    static <T> CatalogServiceProvider<T> fail()
    {
        return fail("Not supported");
    }

    static <T> CatalogServiceProvider<T> fail(String message)
    {
        return catalogName -> {
            throw new IllegalStateException(message);
        };
    }

    static <T> CatalogServiceProvider<T> singleton(CatalogName name, T value)
    {
        return catalogName -> {
            checkArgument(catalogName.equals(name));
            return value;
        };
    }

    @NotNull
    T getService(CatalogName catalogName);
}
