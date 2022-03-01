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
package io.trino.client.auth.external;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

class Token
{
    private final String token;
    private final Optional<String> refreshToken;
    private final Optional<URI> refreshUri;
    private final Instant issuedAt;

    Token(Token token, Optional<URI> refreshUri)
    {
        this(token.token(), token.getRefreshToken(), refreshUri);
    }

    public Token(String token, Optional<String> refreshToken, Optional<URI> refreshUri)
    {
        this.token = requireNonNull(token, "token is null");
        this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
        this.refreshUri = requireNonNull(refreshUri, "refreshUri is null");
        this.issuedAt = Instant.now();
    }

    public String token()
    {
        return token;
    }

    public Optional<String> getRefreshToken()
    {
        return refreshToken;
    }

    public Optional<URI> getRefreshUri()
    {
        return refreshUri;
    }

    public boolean isExpired(int expiresAfter)
    {
        return Instant.now().isAfter(issuedAt.plus(expiresAfter, SECONDS));
    }
}
