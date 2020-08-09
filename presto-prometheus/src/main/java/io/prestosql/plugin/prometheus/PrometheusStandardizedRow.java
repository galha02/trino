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
package io.prestosql.plugin.prometheus;

import io.prestosql.spi.block.Block;

import java.sql.Timestamp;

import static java.util.Objects.requireNonNull;

public class PrometheusStandardizedRow
{
    private final Block labels;
    private final Timestamp timestamp;
    private final Double value;

    public PrometheusStandardizedRow(Block labels, Timestamp timestamp, Double value)
    {
        this.labels = requireNonNull(labels, "labels is null");
        this.timestamp = requireNonNull(timestamp, "timestamp is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Block getLabels()
    {
        return labels;
    }

    public Timestamp getTimestamp()
    {
        return timestamp;
    }

    public Double getValue()
    {
        return value;
    }
}
