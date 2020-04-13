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
package io.prestosql.server.ui;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static io.prestosql.server.ui.WebUiUtil.getUiLocation;
import static io.prestosql.server.ui.WebUiUtil.sendRedirect;

public class NoAuthWebUiAuthenticationManager
        implements WebUiAuthenticationManager
{
    @Override
    public void handleUiRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
            throws IOException, ServletException
    {
        if (request.getPathInfo() == null
                || request.getPathInfo().equals("/")
                || request.getPathInfo().equals("/ui/logout")) {
            sendRedirect(response, getUiLocation(request));
            return;
        }

        nextFilter.doFilter(request, response);
        return;
    }
}
