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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.getResource;

@Test
public class BaseOracleKerberosImpersonationWithAuthToLocalTest
        extends BaseOracleImpersonationWithAuthToLocal
{
    public BaseOracleKerberosImpersonationWithAuthToLocalTest(Map<String, String> additionalProperties)
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("oracle.impersonation.enabled", "true")
                        .put("auth-to-local.config-file", getResource("auth-to-local.json").toString())
                        .put("auth-to-local.refresh-period", "1s")
                        .put("oracle.synonyms.enabled", "true")
                        .put("oracle.authentication.type", "KERBEROS")
                        .put("kerberos.client.principal", "test@TESTING-KRB.STARBURSTDATA.COM")
                        .put("kerberos.client.keytab", getResource("krb/client/test.keytab").toString())
                        .put("kerberos.config", getResource("krb/krb5.conf").toString())
                        .build(),
                session -> createSession(session.getIdentity().getUser() + "/admin@company.com"),
                ImmutableList.of()));
    }

    @Override
    protected String getProxyUser()
    {
        return "test";
    }
}
