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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Revoke
        extends Statement
{
    private final boolean grantOptionFor;
    private final Optional<List<String>> privileges; // missing means ALL PRIVILEGES
    private final GrantObject grantObject;
    private final PrincipalSpecification grantee;

    public Revoke(boolean grantOptionFor, Optional<List<String>> privileges, GrantObject grantObject, PrincipalSpecification grantee)
    {
        this(Optional.empty(), grantOptionFor, privileges, grantObject, grantee);
    }

    public Revoke(NodeLocation location, boolean grantOptionFor, Optional<List<String>> privileges, GrantObject grantObject, PrincipalSpecification grantee)
    {
        this(Optional.of(location), grantOptionFor, privileges, grantObject, grantee);
    }

    private Revoke(Optional<NodeLocation> location, boolean grantOptionFor, Optional<List<String>> privileges, GrantObject grantObject, PrincipalSpecification grantee)
    {
        super(location);
        this.grantOptionFor = grantOptionFor;
        requireNonNull(privileges, "privileges is null");
        this.privileges = privileges.map(ImmutableList::copyOf);
        this.grantObject = requireNonNull(grantObject, "grantScope is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
    }

    public boolean isGrantOptionFor()
    {
        return grantOptionFor;
    }

    public Optional<List<String>> getPrivileges()
    {
        return privileges;
    }

    public GrantObject getGrantObject()
    {
        return grantObject;
    }

    public PrincipalSpecification getGrantee()
    {
        return grantee;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRevoke(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(grantOptionFor, privileges, grantObject, grantee);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Revoke o = (Revoke) obj;
        return grantOptionFor == o.grantOptionFor &&
                Objects.equals(privileges, o.privileges) &&
                Objects.equals(grantObject, o.grantObject) &&
                Objects.equals(grantee, o.grantee);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("grantOptionFor", grantOptionFor)
                .add("privileges", privileges)
                .add("grantScope", grantObject)
                .add("grantee", grantee)
                .toString();
    }
}
