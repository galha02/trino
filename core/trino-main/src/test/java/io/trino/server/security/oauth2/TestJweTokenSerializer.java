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
package io.trino.server.security.oauth2;

import com.nimbusds.jose.KeyLengthException;
import io.airlift.units.Duration;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import org.testng.annotations.Test;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static io.airlift.units.Duration.succinctDuration;
import static io.trino.server.security.oauth2.TokenPairSerializer.TokenPair.accessAndRefreshTokens;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJweTokenSerializer
{
    @Test
    public void testSerialization()
            throws Exception
    {
        JweTokenSerializer serializer = tokenSerializer(Clock.systemUTC(), succinctDuration(5, SECONDS));

        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(accessAndRefreshTokens("access_token", expiration, "refresh_token"));
        TokenPair deserializedTokenPair = serializer.deserialize(serializedTokenPair);

        assertThat(deserializedTokenPair.getAccessToken()).isEqualTo("access_token");
        assertThat(deserializedTokenPair.getExpiration()).isEqualTo(expiration);
        assertThat(deserializedTokenPair.getRefreshToken()).isEqualTo(Optional.of("refresh_token"));
    }

    @Test
    public void testTokenDeserializationAfterTimeoutButBeforeExpirationExtension()
            throws Exception
    {
        TestingClock clock = new TestingClock();
        JweTokenSerializer serializer = tokenSerializer(
                clock,
                succinctDuration(12, MINUTES));
        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(accessAndRefreshTokens("access_token", expiration, "refresh_token"));
        clock.advanceBy(succinctDuration(10, MINUTES));
        TokenPair deserializedTokenPair = serializer.deserialize(serializedTokenPair);

        assertThat(deserializedTokenPair.getAccessToken()).isEqualTo("access_token");
        assertThat(deserializedTokenPair.getExpiration()).isEqualTo(expiration);
        assertThat(deserializedTokenPair.getRefreshToken()).isEqualTo(Optional.of("refresh_token"));
    }

    @Test
    public void testTokenDeserializationAfterTimeoutAndExpirationExtension()
            throws Exception
    {
        TestingClock clock = new TestingClock();

        JweTokenSerializer serializer = tokenSerializer(
                clock,
                succinctDuration(12, MINUTES));
        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(accessAndRefreshTokens("access_token", expiration, "refresh_token"));

        clock.advanceBy(succinctDuration(20, MINUTES));
        assertThatThrownBy(() -> serializer.deserialize(serializedTokenPair))
                .isExactlyInstanceOf(ExpiredJwtException.class);
    }

    private JweTokenSerializer tokenSerializer(Clock clock, Duration tokenExpiration)
            throws GeneralSecurityException, KeyLengthException
    {
        return new JweTokenSerializer(
                new RefreshTokensConfig(),
                new Oauth2ClientStub(),
                "trino_coordinator_test_version",
                "trino_coordinator",
                "sub",
                clock,
                tokenExpiration);
    }

    static class Oauth2ClientStub
            implements OAuth2Client
    {
        private final Map<String, Object> claims = Jwts.claims()
                .setSubject("user");

        @Override
        public void load()
        {
        }

        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }

        @Override
        public Optional<Map<String, Object>> getClaims(String accessToken)
        {
            return Optional.of(claims);
        }

        @Override
        public Response refreshTokens(String refreshToken)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }
    }
}
