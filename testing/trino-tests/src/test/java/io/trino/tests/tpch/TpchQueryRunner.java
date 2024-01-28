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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.QueryRunner;

public final class TpchQueryRunner
{
    private TpchQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("sql.default-catalog", "tpch")
                        .put("sql.default-schema", "tiny")
                        .buildOrThrow())
                .build();
        Thread.sleep(10);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
