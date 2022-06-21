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
package io.trino.plugin.hive.azure;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;

import java.util.Optional;

public class HiveAzureConfig
{
    private String wasbStorageAccount;
    private String wasbAccessKey;
    private String abfsStorageAccount;
    private String abfsAccessKey;
    private String adlClientId;
    private String adlCredential;
    private String adlRefreshUrl;
    private HostAndPort adlProxyHost;
    private String abfsOAuthClientEndpoint;
    private String abfsOAuthClientId;
    private String abfsOAuthClientSecret;

    public Optional<String> getWasbStorageAccount()
    {
        return Optional.ofNullable(wasbStorageAccount);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.wasb-storage-account")
    @Config("azure.wasb.storage-account")
    public HiveAzureConfig setWasbStorageAccount(String wasbStorageAccount)
    {
        this.wasbStorageAccount = wasbStorageAccount;
        return this;
    }

    public Optional<String> getWasbAccessKey()
    {
        return Optional.ofNullable(wasbAccessKey);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.wasb-access-key")
    @Config("azure.wasb.access-key")
    public HiveAzureConfig setWasbAccessKey(String wasbAccessKey)
    {
        this.wasbAccessKey = wasbAccessKey;
        return this;
    }

    public Optional<String> getAbfsStorageAccount()
    {
        return Optional.ofNullable(abfsStorageAccount);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.abfs-storage-account")
    @Config("azure.abfs.storage-account")
    public HiveAzureConfig setAbfsStorageAccount(String abfsStorageAccount)
    {
        this.abfsStorageAccount = abfsStorageAccount;
        return this;
    }

    public Optional<String> getAbfsAccessKey()
    {
        return Optional.ofNullable(abfsAccessKey);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.abfs-access-key")
    @Config("azure.abfs.access-key")
    public HiveAzureConfig setAbfsAccessKey(String abfsAccessKey)
    {
        this.abfsAccessKey = abfsAccessKey;
        return this;
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.adl-client-id")
    @Config("azure.adl.client-id")
    public HiveAzureConfig setAdlClientId(String adlClientId)
    {
        this.adlClientId = adlClientId;
        return this;
    }

    public Optional<String> getAdlClientId()
    {
        return Optional.ofNullable(adlClientId);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.adl-credential")
    @Config("azure.adl.credential")
    public HiveAzureConfig setAdlCredential(String adlCredential)
    {
        this.adlCredential = adlCredential;
        return this;
    }

    public Optional<String> getAdlCredential()
    {
        return Optional.ofNullable(adlCredential);
    }

    public Optional<String> getAdlRefreshUrl()
    {
        return Optional.ofNullable(adlRefreshUrl);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.adl-refresh-url")
    @Config("azure.adl.refresh-url")
    public HiveAzureConfig setAdlRefreshUrl(String adlRefreshUrl)
    {
        this.adlRefreshUrl = adlRefreshUrl;
        return this;
    }

    @LegacyConfig("hive.azure.adl-proxy-host")
    @Config("azure.adl.proxy-host")
    public HiveAzureConfig setAdlProxyHost(HostAndPort adlProxyHost)
    {
        this.adlProxyHost = adlProxyHost;
        return this;
    }

    public Optional<HostAndPort> getAdlProxyHost()
    {
        return Optional.ofNullable(adlProxyHost);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.abfs.oauth.endpoint")
    @Config("azure.abfs.oauth.endpoint")
    public HiveAzureConfig setAbfsOAuthClientEndpoint(String endpoint)
    {
        abfsOAuthClientEndpoint = endpoint;
        return this;
    }

    public Optional<String> getAbfsOAuthClientEndpoint()
    {
        return Optional.ofNullable(abfsOAuthClientEndpoint);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.abfs.oauth.client-id")
    @Config("azure.abfs.oauth.client-id")
    public HiveAzureConfig setAbfsOAuthClientId(String id)
    {
        abfsOAuthClientId = id;
        return this;
    }

    public Optional<String> getAbfsOAuthClientId()
    {
        return Optional.ofNullable(abfsOAuthClientId);
    }

    @ConfigSecuritySensitive
    @LegacyConfig("hive.azure.abfs.oauth.secret")
    @Config("azure.abfs.oauth.secret")
    public HiveAzureConfig setAbfsOAuthClientSecret(String secret)
    {
        abfsOAuthClientSecret = secret;
        return this;
    }

    public Optional<String> getAbfsOAuthClientSecret()
    {
        return Optional.ofNullable(abfsOAuthClientSecret);
    }
}
