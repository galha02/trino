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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record WindowFunction(
        String name,
        WindowNode.Frame frame,
        List<Expression> arguments)
{
    public WindowFunction(String name, WindowNode.Frame frame, List<Expression> arguments)
    {
        this.name = requireNonNull(name, "name is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.arguments = ImmutableList.copyOf(arguments);
    }
}
