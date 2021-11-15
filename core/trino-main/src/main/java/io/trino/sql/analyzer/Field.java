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
package io.trino.sql.analyzer;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.type.Type;
import io.trino.sql.tree.QualifiedName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Field
{
    private final Optional<QualifiedObjectName> originTable;
    private final Optional<String> originColumnName;
    private final Optional<QualifiedName> relationAlias;
    private final Optional<String> name;
    private final Optional<String> mappedName;
    private final Type type;
    private final boolean hidden;
    private final boolean aliased;

    public static Field newUnqualified(String name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return new Field(Optional.empty(), Optional.of(name), Optional.empty(), type, false, Optional.empty(), Optional.empty(), false);
    }

    public static Field newUnqualified(Optional<String> name, Type type)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");

        return new Field(Optional.empty(), name, Optional.empty(), type, false, Optional.empty(), Optional.empty(), false);
    }

    public static Field newUnqualified(Optional<String> name, Type type, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");

        return new Field(Optional.empty(), name, Optional.empty(), type, false, originTable, originColumn, aliased);
    }

    public static Field newQualified(QualifiedName relationAlias, Optional<String> name, Type type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originColumn, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");

        return new Field(Optional.of(relationAlias), name, Optional.empty(), type, hidden, originTable, originColumn, aliased);
    }

    public Field(Optional<QualifiedName> relationAlias, Optional<String> name, Optional<String> mappedName, Type type, boolean hidden, Optional<QualifiedObjectName> originTable, Optional<String> originColumnName, boolean aliased)
    {
        requireNonNull(relationAlias, "relationAlias is null");
        requireNonNull(name, "name is null");
        requireNonNull(mappedName, "mappedName is null");
        requireNonNull(type, "type is null");
        requireNonNull(originTable, "originTable is null");
        requireNonNull(originColumnName, "originColumnName is null");

        this.relationAlias = relationAlias;
        this.name = name;
        this.mappedName = mappedName;
        this.type = type;
        this.hidden = hidden;
        this.originTable = originTable;
        this.originColumnName = originColumnName;
        this.aliased = aliased;
    }

    public Optional<QualifiedObjectName> getOriginTable()
    {
        return originTable;
    }

    public Optional<String> getOriginColumnName()
    {
        return originColumnName;
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Optional<String> getMappedName()
    {
        return mappedName;
    }

    public Optional<String> getMappedNameOrName()
    {
        if (mappedName.isPresent()) {
            return mappedName;
        }
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isAliased()
    {
        return aliased;
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return prefix.isEmpty() || relationAlias.isPresent() && relationAlias.get().hasSuffix(prefix.get());
    }

    /*
      Namespaces can have names such as "x", "x.y" or "" if there's no name
      Name to resolve can have names like "a", "x.a", "x.y.a"

      namespace  name     possible match
       ""         "a"           y
       "x"        "a"           y
       "x.y"      "a"           y

       ""         "x.a"         n
       "x"        "x.a"         y
       "x.y"      "x.a"         n

       ""         "x.y.a"       n
       "x"        "x.y.a"       n
       "x.y"      "x.y.a"       n

       ""         "y.a"         n
       "x"        "y.a"         n
       "x.y"      "y.a"         y
     */
    public boolean canResolve(QualifiedName name)
    {
        Optional<String> mappedNameOrName = getMappedNameOrName();
        if (mappedNameOrName.isEmpty()) {
            return false;
        }

        // TODO: need to know whether the qualified name and the name of this field were quoted
        return matchesPrefix(name.getPrefix()) && mappedNameOrName.get().equalsIgnoreCase(name.getSuffix());
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if (relationAlias.isPresent()) {
            result.append(relationAlias.get())
                    .append(".");
        }

        result.append(name.orElse("<anonymous>"))
                .append(":")
                .append(type);

        return result.toString();
    }
}
