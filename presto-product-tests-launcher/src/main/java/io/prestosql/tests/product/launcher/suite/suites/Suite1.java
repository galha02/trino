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
package io.prestosql.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.env.environment.Multinode;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteConfig;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite1
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(SuiteConfig config)
    {
        return ImmutableList.of(
            /**
             * presto-product-tests-launcher/bin/run-launcher test run \
             *     --environment multinode \
             *     -- \
             *     -x big_query,storage_formats,profile_specific_tests,tpcds,hive_compression,"${DISTRO_SKIP_GROUP}" \
             *     -e "${DISTRO_SKIP_TEST}" \
             *     || suite_exit_code=1
             */
            testOnEnvironment(Multinode.class)
                    .withExcludedGroups("big_query", "storage_formats", "profile_specific_tests", "tpcds", "hive_compression")
                    .build());
    }
}
