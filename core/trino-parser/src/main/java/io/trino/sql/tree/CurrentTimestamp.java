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

import static java.util.Objects.requireNonNull;

public final class CurrentTimestamp
        extends Expression
{
    private final Optional<Integer> precision;

    public CurrentTimestamp(NodeLocation location, Optional<Integer> precision)
    {
        super(Optional.of(location));

        requireNonNull(precision, "precision is null");
        this.precision = precision;
    }

    public Optional<Integer> getPrecision()
    {
        return precision;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCurrentTimestamp(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        CurrentTimestamp that = (CurrentTimestamp) o;
        return Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        CurrentTimestamp otherNode = (CurrentTimestamp) other;
        return Objects.equals(precision, otherNode.precision);
    }
}
