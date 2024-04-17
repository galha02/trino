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
package io.trino.tests.tpch;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TpchQueryRunner
{
    private TpchQueryRunner() {}

    public static TpchQueryRunnerBuilder builder()
    {
        return new TpchQueryRunnerBuilder();
    }

    public static final class TpchQueryRunnerBuilder
            extends DistributedQueryRunner.Builder<TpchQueryRunnerBuilder>
    {
        private Map<String, String> connectorProperties = ImmutableMap.of();

        private TpchQueryRunnerBuilder()
        {
            super(testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build());
        }

        @CanIgnoreReturnValue
        public TpchQueryRunnerBuilder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = ImmutableMap.copyOf(connectorProperties);
            return this;
        }

        @Override
        protected void configure(DistributedQueryRunner queryRunner)
        {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", connectorProperties);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = builder()
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("sql.default-catalog", "tpch")
                        .put("sql.default-schema", "tiny")
                        .buildOrThrow())
                .build();
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
