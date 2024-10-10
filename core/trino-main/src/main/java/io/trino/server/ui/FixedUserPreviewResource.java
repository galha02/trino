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
package io.trino.server.ui;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_PREVIEW_AUTH_INFO;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("")
public class FixedUserPreviewResource
{
    private final FixedUserWebUiConfig fixedUserWebUiConfig;

    @Inject
    public FixedUserPreviewResource(FixedUserWebUiConfig fixedUserWebUiConfig)
    {
        this.fixedUserWebUiConfig = requireNonNull(fixedUserWebUiConfig, "fixedUserWebUiConfig is null");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path(UI_PREVIEW_AUTH_INFO)
    @Produces(APPLICATION_JSON)
    public AuthInfo getAuthInfo(ContainerRequestContext request, @Context SecurityContext securityContext)
    {
        return new AuthInfo("fixed", false, true, Optional.of(fixedUserWebUiConfig.getUsername()));
    }
}
