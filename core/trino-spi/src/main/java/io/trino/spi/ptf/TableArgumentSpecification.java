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
package io.trino.spi.ptf;

public class TableArgumentSpecification
        extends ArgumentSpecification
{
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;

    private TableArgumentSpecification(String name, boolean rowSemantics, boolean pruneWhenEmpty, boolean passThroughColumns)
    {
        super(name, true, null);

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
    }

    public boolean isRowSemantics()
    {
        return rowSemantics;
    }

    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    public boolean isPassThroughColumns()
    {
        return passThroughColumns;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private boolean rowSemantics;
        private boolean pruneWhenEmpty;
        private boolean passThroughColumns;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder rowSemantics()
        {
            this.rowSemantics = true;
            return this;
        }

        public Builder pruneWhenEmpty()
        {
            this.pruneWhenEmpty = true;
            return this;
        }

        public Builder passThroughColumns()
        {
            this.passThroughColumns = true;
            return this;
        }

        public TableArgumentSpecification build()
        {
            return new TableArgumentSpecification(name, rowSemantics, pruneWhenEmpty, passThroughColumns);
        }
    }
}
