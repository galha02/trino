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
package io.trino.testing.assertions;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class TestUtil
{
    private TestUtil() {}

    public static <T> void verifyResultOrFailure(Supplier<T> callback, Consumer<T> verifyResults, Consumer<Throwable> verifyFailure)
    {
        requireNonNull(callback, "callback is null");
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");

        T result;
        try {
            result = callback.get();
        }
        catch (Throwable t) {
            verifyFailure.accept(t);
            return;
        }
        verifyResults.accept(result);
    }
}
