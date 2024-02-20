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
package io.trino.operator.table.json;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public record JsonTablePlanUnion(List<JsonTablePlanNode> siblings)
        implements JsonTablePlanNode
{
    public JsonTablePlanUnion(List<JsonTablePlanNode> siblings)
    {
        this.siblings = ImmutableList.copyOf(siblings);
        checkArgument(siblings.size() >= 2, "less than 2 siblings in Union node");
    }
}
