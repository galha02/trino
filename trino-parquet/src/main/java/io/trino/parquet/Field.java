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
package io.trino.parquet;

import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public abstract class Field
{
    private final Type type;
    private final int repetitionLevel;
    private final int definitionLevel;
    private final boolean required;

    protected Field(Type type, int repetitionLevel, int definitionLevel, boolean required)
    {
        this.type = requireNonNull(type, "type is required");
        this.repetitionLevel = repetitionLevel;
        this.definitionLevel = definitionLevel;
        this.required = required;
    }

    public Type getType()
    {
        return type;
    }

    public int getRepetitionLevel()
    {
        return repetitionLevel;
    }

    public int getDefinitionLevel()
    {
        return definitionLevel;
    }

    public boolean isRequired()
    {
        return required;
    }

    @Override
    public abstract String toString();
}
