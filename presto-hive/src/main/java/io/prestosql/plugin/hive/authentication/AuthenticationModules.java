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
package io.prestosql.plugin.hive.authentication;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.prestosql.plugin.base.authentication.KerberosAuthentication;
import io.prestosql.plugin.hive.ForHdfs;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;

import javax.inject.Inject;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.hive.authentication.KerberosHadoopAuthentication.createKerberosHadoopAuthentication;

public final class AuthenticationModules
{
    private AuthenticationModules() {}

    public static Module noHdfsAuthenticationModule()
    {
        return binder -> binder
                .bind(HdfsAuthentication.class)
                .to(NoHdfsAuthentication.class)
                .in(SINGLETON);
    }

    public static Module simpleImpersonatingHdfsAuthenticationModule()
    {
        return binder -> {
            binder.bind(HadoopAuthentication.class).annotatedWith(ForHdfs.class).to(SimpleHadoopAuthentication.class);
            binder.bind(HdfsAuthentication.class).to(ImpersonatingHdfsAuthentication.class).in(SINGLETON);
        };
    }

    public static Module kerberosHdfsAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HdfsAuthentication.class)
                        .to(DirectHdfsAuthentication.class)
                        .in(SINGLETON);
                configBinder(binder).bindConfig(HdfsKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HdfsKerberosConfig config, HdfsConfigurationInitializer updater)
            {
                String principal = config.getHdfsPrestoPrincipal();
                String keytabLocation = config.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation, updater);
            }
        };
    }

    public static Module kerberosImpersonatingHdfsAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HdfsAuthentication.class)
                        .to(ImpersonatingHdfsAuthentication.class)
                        .in(SINGLETON);
                configBinder(binder).bindConfig(HdfsKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            @ForHdfs
            HadoopAuthentication createHadoopAuthentication(HdfsKerberosConfig config, HdfsConfigurationInitializer updater)
            {
                String principal = config.getHdfsPrestoPrincipal();
                String keytabLocation = config.getHdfsPrestoKeytab();
                return createCachingKerberosHadoopAuthentication(principal, keytabLocation, updater);
            }
        };
    }

    public static HadoopAuthentication createCachingKerberosHadoopAuthentication(String principal, String keytabLocation, HdfsConfigurationInitializer updater)
    {
        KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(principal, keytabLocation);
        KerberosHadoopAuthentication kerberosHadoopAuthentication = createKerberosHadoopAuthentication(kerberosAuthentication, updater);
        return new CachingKerberosHadoopAuthentication(kerberosHadoopAuthentication);
    }
}
