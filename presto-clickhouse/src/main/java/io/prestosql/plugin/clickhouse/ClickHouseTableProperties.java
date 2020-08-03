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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Clickhouse connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo (a VARCHAR , b INT) WITH (ENGINE='Log');</pre>
 * </p>
 */
public final class ClickHouseTableProperties
{
    public static final String ENGINE = "engine";
    public static final String DEFAULT_TABLE_ENGINE = "Log";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ClickHouseTableProperties()
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        ENGINE,
                        "ClickHouse Table Engine, defaults to Log",
                        DEFAULT_TABLE_ENGINE,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        String value = (String) tableProperties.get(DEFAULT_TABLE_ENGINE);
        return Optional.ofNullable(value);

    }
}
