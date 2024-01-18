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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.testing.ConfigAssertions;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientTlsConfig;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.ssl.TruststoreType.JKS;
import static io.trino.plugin.base.ssl.TruststoreType.PKCS12;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotGrpcServerQueryClientTlsConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotGrpcServerQueryClientTlsConfig.class)
                        .setSslProvider("JDK")
                        .setKeystoreType(JKS)
                        .setTruststoreType(JKS)
                        .setKeystorePath(null)
                        .setKeystorePassword(null)
                        .setTruststorePath(null)
                        .setTruststorePassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("keystore-type", "PKCS12")
                .put("keystore-path", keystoreFile.toString())
                .put("keystore-password", "password")
                .put("truststore-type", "PKCS12")
                .put("truststore-path", truststoreFile.toString())
                .put("truststore-password", "password")
                .put("ssl-provider", "OPENSSL")
                .buildOrThrow();
        PinotGrpcServerQueryClientTlsConfig expected = (PinotGrpcServerQueryClientTlsConfig) new PinotGrpcServerQueryClientTlsConfig()
                .setSslProvider("OPENSSL")
                .setKeystoreType(PKCS12)
                .setTruststoreType(PKCS12)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("password");
        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testFailOnMissingKeystorePasswordWithKeystorePathSet()
            throws Exception
    {
        String secret = "pinot";
        Path keystorePath = Files.createTempFile("keystore", ".p12");

        writeToFile(keystorePath, secret);

        PinotGrpcServerQueryClientTlsConfig config = new PinotGrpcServerQueryClientTlsConfig();
        config.setKeystorePath(keystorePath.toFile());
        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("pinot.grpc.tls.keystore-password must set when pinot.grpc.tls.keystore-path is given");
    }

    @Test
    public void testFailOnMissingTruststorePasswordWithTruststorePathSet()
            throws Exception
    {
        String secret = "pinot";
        Path truststorePath = Files.createTempFile("truststore", ".p12");

        writeToFile(truststorePath, secret);

        PinotGrpcServerQueryClientTlsConfig config = new PinotGrpcServerQueryClientTlsConfig();
        config.setTruststorePath(truststorePath.toFile());
        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("pinot.grpc.tls.truststore-password must set when pinot.grpc.tls.truststore-path is given");
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (FileWriter writer = new FileWriter(filepath.toFile(), UTF_8)) {
            writer.write(content);
        }
    }
}
