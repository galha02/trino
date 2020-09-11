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
import io.prestosql.tests.product.launcher.env.environment.MultinodeTls;
import io.prestosql.tests.product.launcher.env.environment.MultinodeTlsKerberos;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationWithDataProtection;
import io.prestosql.tests.product.launcher.env.environment.SinglenodeKerberosHdfsImpersonationWithWireEncryption;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.prestosql.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite3
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment multinode-tls \
                 *     -- -g smoke,cli,group-by,join,tls -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(MultinodeTls.class)
                        .withGroups("smoke", "cli", "group-by", "join", "tls")
                        .build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment multinode-tls-kerberos \
                 *     -- -g cli,group-by,join,tls -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(MultinodeTlsKerberos.class)
                        .withGroups("cli", "group-by", "join", "tls")
                        .build(),

                /**
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kerberos-hdfs-impersonation-with-wire-encryption \
                 *     -- -g storage_formats,cli,hdfs_impersonation,authorization -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationWithWireEncryption.class)
                        .withGroups("storage_formats", "cli", "hdfs_impersonation,authorization")
                        .build(),

                /**
                 * # Smoke run af a few tests in environment with dfs.data.transfer.protection=true. Arbitrary tests which access HDFS data were chosen.
                 * presto-product-tests-launcher/bin/run-launcher test run \
                 *     --environment singlenode-kerberos-hdfs-impersonation-with-data-protection \
                 *     -- -t TestHiveStorageFormats.testOrcTableCreatedInPresto,TestHiveCreateTable.testCreateTable  -x "${DISTRO_SKIP_GROUP}" -e "${DISTRO_SKIP_TEST}" \
                 *     || suite_exit_code=1
                 */
                testOnEnvironment(SinglenodeKerberosHdfsImpersonationWithDataProtection.class)
                        .withTests("TestHiveStorageFormats.testOrcTableCreatedInPresto", "TestHiveCreateTable.testCreateTable")
                        .build());
    }
}
