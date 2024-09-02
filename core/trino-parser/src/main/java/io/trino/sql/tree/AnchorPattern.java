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

public class AnchorPattern
        extends RowPattern
{
    public enum Type
    {
        PARTITION_START,
        PARTITION_END
    }

    private final Type type;

    public AnchorPattern(NodeLocation location, Type type)
    {
        super(Optional.of(location));
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAnchorPattern(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AnchorPattern o = (AnchorPattern) obj;
        return Objects.equals(type, o.type);
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
