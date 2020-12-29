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
package io.trino.spi.connector;

import static java.util.Objects.requireNonNull;

public class SchemaNotFoundException
        extends NotFoundException
{
    private final String schemaName;

    public SchemaNotFoundException(String schemaName)
    {
        this(schemaName, "Schema " + schemaName + " not found");
    }

    public SchemaNotFoundException(String schemaName, String message)
    {
        super(message);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public SchemaNotFoundException(String schemaName, Throwable cause)
    {
        this(schemaName, "Schema " + schemaName + " not found", cause);
    }

    public SchemaNotFoundException(String schemaName, String message, Throwable cause)
    {
        super(message, cause);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public String getSchemaName()
    {
        return schemaName;
    }
}
