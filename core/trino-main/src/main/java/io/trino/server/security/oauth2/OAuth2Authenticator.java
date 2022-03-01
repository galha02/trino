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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getInitiateUri;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getRefreshTokenUri;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getTokenUri;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        extends AbstractBearerAuthenticator
{
    private final OAuth2Service service;
    private final String principalField;
    private final Optional<String> groupsField;
    private final UserMapping userMapping;

    @Inject
    public OAuth2Authenticator(OAuth2Service service, OAuth2Config config)
    {
        this.service = requireNonNull(service, "service is null");
        this.principalField = config.getPrincipalField();
        groupsField = requireNonNull(config.getGroupsField(), "groupsField is null");
        userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
    }

    @Override
    protected Optional<Identity> createIdentity(String token)
            throws UserMappingException
    {
        try {
            Optional<Map<String, Object>> claims = service.convertTokenToClaims(token);
            if (claims.isEmpty()) {
                return Optional.empty();
            }
            String principal = (String) claims.get().get(principalField);
            Identity.Builder builder = Identity.forUser(userMapping.mapUser(principal));
            builder.withPrincipal(new BasicPrincipal(principal));
            groupsField.flatMap(field -> Optional.ofNullable((List<String>) claims.get().get(field)))
                    .ifPresent(groups -> builder.withGroups(ImmutableSet.copyOf(groups)));
            return Optional.of(builder.build());
        }
        catch (ChallengeFailedException e) {
            return Optional.empty();
        }
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        return createAuthenticationException(request, message, Optional.empty());
    }

    @Override
    protected AuthenticationException needsTokenRefresh(ContainerRequestContext request, String message)
    {
        return createAuthenticationException(request, message, Optional.of(resolveRefreshTokenUri(request)));
    }

    private AuthenticationException createAuthenticationException(ContainerRequestContext request, String message, Optional<URI> refreshTokenUri)
    {
        UUID authId = UUID.randomUUID();
        ImmutableMap.Builder<String, URI> attributes = ImmutableMap.<String, URI>builder()
                .put("x_redirect_server", resolveTokenUri(request, getInitiateUri(authId)))
                .put("x_token_server", resolveTokenUri(request, getTokenUri(authId)));
        refreshTokenUri.ifPresent(uri -> attributes.put("x_token_refresh_server", uri));
        String authenticateHeader = attributes.buildOrThrow().entrySet().stream()
                .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                .collect(Collectors.joining(", ", "Bearer ", ""));
        return new AuthenticationException(message, authenticateHeader);
    }

    private URI resolveRefreshTokenUri(ContainerRequestContext request)
    {
        return resolveTokenUri(request, getRefreshTokenUri());
    }

    private URI resolveTokenUri(ContainerRequestContext request, String path)
    {
        return request.getUriInfo().getBaseUri().resolve(path);
    }
}
