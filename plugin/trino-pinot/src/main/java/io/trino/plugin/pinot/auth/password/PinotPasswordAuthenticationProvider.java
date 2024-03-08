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
package io.trino.plugin.pinot.auth.password;

import io.trino.plugin.pinot.auth.PinotAuthenticationProvider;

import java.util.Base64;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

public class PinotPasswordAuthenticationProvider
        implements PinotAuthenticationProvider
{
    private final Optional<String> authToken;

    public PinotPasswordAuthenticationProvider(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        this.authToken = Optional.of(encode(user, password));
    }

    @Override
    public Optional<String> getAuthenticationToken()
    {
        return authToken;
    }

    public static String encode(String username, String password)
    {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(ISO_8859_1));
    }
}
