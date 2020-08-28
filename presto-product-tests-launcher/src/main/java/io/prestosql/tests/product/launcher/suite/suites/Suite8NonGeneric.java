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
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentDefaults;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeHdp3;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite8NonGeneric
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        return ImmutableList.of(
                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *    --environment singlenode-hdp3 \
                 *     -- -g hdp3_only,hive_transactional \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeHdp3.class).withGroups("hdp3_only", "hive_transactional").build());
    }
}
