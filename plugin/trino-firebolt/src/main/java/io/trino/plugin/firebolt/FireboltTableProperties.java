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
package io.trino.plugin.firebolt;

import com.firebolt.shadow.com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class FireboltTableProperties
        implements TablePropertiesProvider
{
    public static final String TYPE = "type";

    private final List<PropertyMetadata<?>> tableProperties = ImmutableList.of(
            enumProperty(
                    TYPE,
                    "Table type",
                    TableType.class,
                    TableType.DIMENSION,
                    false));

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<TableType> getTableType(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable(tableProperties.get(TYPE)).map(TableType.class::cast);
    }
}
