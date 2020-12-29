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
package io.trino.plugin.session;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.session.db.DbSessionPropertyManagerFactory;
import io.trino.plugin.session.file.FileSessionPropertyManagerFactory;
import io.trino.spi.Plugin;
import io.trino.spi.session.SessionPropertyConfigurationManagerFactory;

public class SessionPropertyConfigurationManagerPlugin
        implements Plugin
{
    @Override
    public Iterable<SessionPropertyConfigurationManagerFactory> getSessionPropertyConfigurationManagerFactories()
    {
        return ImmutableList.of(
                new FileSessionPropertyManagerFactory(),
                new DbSessionPropertyManagerFactory());
    }
}
