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
package io.trino.memory;

import io.trino.server.security.ResourceSecurity;
import io.trino.spi.memory.MemoryPoolInfo;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Manages memory pools on this worker node
 */
@Path("/v1/memory")
public class MemoryResource
{
    private final LocalMemoryManager memoryManager;

    @Inject
    public MemoryResource(LocalMemoryManager memoryManager)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MemoryInfo getMemoryInfo()
    {
        return memoryManager.getInfo();
    }

    @ResourceSecurity(MANAGEMENT_READ)
    @GET
    @Path("{poolId}")
    public Response getMemoryInfo(@PathParam("poolId") String poolId)
    {
        if (GENERAL_POOL.getId().equals(poolId)) {
            return toSuccessfulResponse(memoryManager.getGeneralPool().getInfo());
        }

        return Response.status(NOT_FOUND).build();
    }

    private Response toSuccessfulResponse(MemoryPoolInfo memoryInfo)
    {
        return Response.ok()
                .entity(memoryInfo)
                .build();
    }
}
