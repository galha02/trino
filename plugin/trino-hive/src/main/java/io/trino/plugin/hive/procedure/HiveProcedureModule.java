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
package io.trino.plugin.hive.procedure;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class HiveProcedureModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(CreateEmptyPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RegisterPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(SyncPartitionMetadataProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(DropStatsProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(AlterTableLocationProcedure.class).in(Scopes.SINGLETON);
    }
}
