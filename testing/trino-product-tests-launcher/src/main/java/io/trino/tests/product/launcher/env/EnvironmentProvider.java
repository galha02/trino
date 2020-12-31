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
package io.trino.tests.product.launcher.env;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.tests.product.launcher.env.common.EnvironmentExtender;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;

public abstract class EnvironmentProvider
        implements EnvironmentExtender
{
    private static final Logger log = Logger.get(EnvironmentProvider.class);
    private final List<EnvironmentExtender> bases;

    protected EnvironmentProvider(EnvironmentExtender... bases)
    {
        this(ImmutableList.copyOf(requireNonNull(bases, "bases is null")));
    }

    protected EnvironmentProvider(List<EnvironmentExtender> bases)
    {
        this.bases = ImmutableList.copyOf(requireNonNull(bases, "bases is null"));
    }

    public final Environment.Builder createEnvironment(String name, EnvironmentConfig environmentConfig)
    {
        requireNonNull(environmentConfig, "environmentConfig is null");
        Environment.Builder builder = Environment.builder(name);

        // Environment is created by applying bases, environment definition and environment config to builder
        ImmutableList<EnvironmentExtender> extenders = ImmutableList.<EnvironmentExtender>builder()
                .addAll(bases)
                .add(this)
                .add(environmentConfig)
                .build();

        Set<EnvironmentExtender> seen = newSetFromMap(new IdentityHashMap<>());
        extenders.forEach(extender -> extend(extender, builder, seen));
        return builder;
    }

    private void extend(EnvironmentExtender extender, Environment.Builder builder, Set<EnvironmentExtender> seen)
    {
        extender.getDependencies()
                .forEach(dependencyExtender -> extend(dependencyExtender, builder, seen));
        if (seen.add(extender)) {
            log.info("Building environment %s with extender: %s", builder.getEnvironmentName(), extender.getClass().getSimpleName());
            extender.extendEnvironment(builder);
        }
    }
}
