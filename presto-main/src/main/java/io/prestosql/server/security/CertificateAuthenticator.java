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

import io.prestosql.spi.security.AuthenticatedUser;

import javax.servlet.http.HttpServletRequest;

import java.security.cert.X509Certificate;

public class CertificateAuthenticator
        implements Authenticator
{
    private static final String X509_ATTRIBUTE = "javax.servlet.request.X509Certificate";

    @Override
    public AuthenticatedUser authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute(X509_ATTRIBUTE);
        if ((certs == null) || (certs.length == 0)) {
            throw new AuthenticationException(null);
        }
        return AuthenticatedUser.forPrincipal(certs[0].getSubjectX500Principal());
    }
}
