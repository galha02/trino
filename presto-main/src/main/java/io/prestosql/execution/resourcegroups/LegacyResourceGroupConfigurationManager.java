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
package io.prestosql.execution.resourcegroups;

import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.resourcegroups.LegacyResourceGroupConfigurationManager.VoidContext;
import io.prestosql.spi.resourcegroups.ResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class LegacyResourceGroupConfigurationManager
        implements ResourceGroupConfigurationManager<VoidContext>
{
    enum VoidContext
    {
        NONE
    }

    private static final ResourceGroupId GLOBAL = new ResourceGroupId("global");

    private final int hardConcurrencyLimit;
    private final int maxQueued;

    @Inject
    public LegacyResourceGroupConfigurationManager(QueryManagerConfig config)
    {
        hardConcurrencyLimit = config.getMaxConcurrentQueries();
        maxQueued = config.getMaxQueuedQueries();
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext<VoidContext> criteria)
    {
        checkArgument(group.getId().equals(GLOBAL), "Unexpected resource group: %s", group.getId());
        group.setMaxQueuedQueries(maxQueued);
        group.setHardConcurrencyLimit(hardConcurrencyLimit);
    }

    @Override
    public Optional<SelectionContext<VoidContext>> match(SelectionCriteria criteria)
    {
        return Optional.of(new SelectionContext<>(GLOBAL, VoidContext.NONE));
    }

    @Override
    public SelectionContext<VoidContext> parentGroupContext(SelectionContext<VoidContext> context)
    {
        return new SelectionContext<>(
                context.getResourceGroupId().getParent().orElseThrow(() -> new IllegalArgumentException("Group has no parent group: " + context.getResourceGroupId())),
                VoidContext.NONE);
    }
}
