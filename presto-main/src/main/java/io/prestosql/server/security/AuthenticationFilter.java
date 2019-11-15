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
package io.prestosql.server.security;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import io.prestosql.server.InternalAuthenticationManager;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class AuthenticationFilter
        implements Filter
{
    private static final String HTTPS_PROTOCOL = "https";

    private final List<Authenticator> authenticators;
    private final boolean httpsForwardingEnabled;
    private final InternalAuthenticationManager internalAuthenticationManager;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators, SecurityConfig securityConfig, InternalAuthenticationManager internalAuthenticationManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.httpsForwardingEnabled = requireNonNull(securityConfig, "securityConfig is null").getEnableForwardingHttps();
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (internalAuthenticationManager.isInternalRequest(request)) {
            Principal principal = internalAuthenticationManager.authenticateInternalRequest(request);
            if (principal == null) {
                response.sendError(SC_UNAUTHORIZED);
                return;
            }
            nextFilter.doFilter(withPrincipal(request, principal), response);
            return;
        }

        // skip authentication if non-secure or not configured
        if (!doesRequestSupportAuthentication(request)) {
            nextFilter.doFilter(request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Principal principal;
            try {
                principal = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            nextFilter.doFilter(withPrincipal(request, principal), response);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        response.sendError(SC_UNAUTHORIZED, Joiner.on(" | ").join(messages));
    }

    private boolean doesRequestSupportAuthentication(HttpServletRequest request)
    {
        if (authenticators.isEmpty()) {
            return false;
        }
        if (request.isSecure()) {
            return true;
        }
        return httpsForwardingEnabled && Strings.nullToEmpty(request.getHeader(HttpHeaders.X_FORWARDED_PROTO)).equalsIgnoreCase(HTTPS_PROTOCOL);
    }

    private static ServletRequest withPrincipal(HttpServletRequest request, Principal principal)
    {
        requireNonNull(principal, "principal is null");
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }
        };
    }

    private static void skipRequestBody(HttpServletRequest request)
            throws IOException
    {
        // If we send the challenge without consuming the body of the request,
        // the server will close the connection after sending the response.
        // The client may interpret this as a failed request and not resend the
        // request with the authentication header. We can avoid this behavior
        // in the client by reading and discarding the entire body of the
        // unauthenticated request before sending the response.
        try (InputStream inputStream = request.getInputStream()) {
            copy(inputStream, nullOutputStream());
        }
    }
}
