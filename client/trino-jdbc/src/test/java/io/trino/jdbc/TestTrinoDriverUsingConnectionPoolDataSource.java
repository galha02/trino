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
package io.trino.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoDriverUsingConnectionPoolDataSource
        extends BaseTrinoDriverTest
{
    @Override
    protected Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(url);
        return trinoConnectionPoolDataSource.getPooledConnection("test", null).getConnection();
    }

    @Override
    protected Connection createConnection(String catalog)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s", server.getAddress(), catalog);
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(url);
        return trinoConnectionPoolDataSource.getPooledConnection("test", null).getConnection();
    }

    @Override
    protected Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(url);
        return trinoConnectionPoolDataSource.getPooledConnection("test", null).getConnection();
    }

    @Override
    protected Connection createConnectionWithParameter(String parameter)
            throws SQLException
    {
        String url = format("jdbc:trino://%s?%s", server.getAddress(), parameter);
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(url);
        return trinoConnectionPoolDataSource.getPooledConnection("test", null).getConnection();
    }

    @Test
    @Override
    public void testGetDriverVersion()
            throws Exception
    {
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        Driver driver = trinoConnectionPoolDataSource.getDriver();
        assertThat(driver.getMajorVersion()).isGreaterThan(350);
        assertThat(driver.getMinorVersion()).isEqualTo(0);

        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getDriverName()).isEqualTo("Trino JDBC Driver");
            assertThat(metaData.getDriverVersion()).startsWith(String.valueOf(driver.getMajorVersion()));
            assertThat(metaData.getDriverMajorVersion()).isEqualTo(driver.getMajorVersion());
            assertThat(metaData.getDriverMinorVersion()).isEqualTo(driver.getMinorVersion());
        }
    }

    @Test
    @Override
    public void testNullUrl()
            throws Exception
    {
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(null);

        assertThatThrownBy(() -> trinoConnectionPoolDataSource.getPooledConnection().getConnection())
                .isInstanceOf(SQLException.class)
                .hasMessage("URL is null");
    }

    @Test
    @Override
    public void testDriverPropertyInfoEmpty()
            throws Exception
    {
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(jdbcUrl());

        Properties properties = new Properties();
        DriverPropertyInfo[] infos = trinoConnectionPoolDataSource.getDriver().getPropertyInfo(jdbcUrl(), properties);

        assertThat(infos)
                .extracting(BaseTrinoDriverTest::driverPropertyInfoToString)
                .contains("{name=user, required=false}")
                .contains("{name=password, required=false}")
                .contains("{name=accessToken, required=false}")
                .contains("{name=SSL, value=false, required=false, choices=[true, false]}");
    }

    @Test
    @Override
    public void testDriverPropertyInfoSslEnabled()
            throws Exception
    {
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = new TrinoConnectionPoolDataSource();
        trinoConnectionPoolDataSource.setUrl(jdbcUrl());

        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        DriverPropertyInfo[] infos = trinoConnectionPoolDataSource.getDriver().getPropertyInfo(jdbcUrl(), properties);

        assertThat(infos)
                .extracting(BaseTrinoDriverTest::driverPropertyInfoToString)
                .contains("{name=user, value=test, required=false}")
                .contains("{name=SSL, value=true, required=false, choices=[true, false]}")
                .contains("{name=SSLVerification, value=FULL, required=false, choices=[FULL, CA, NONE]}")
                .contains("{name=SSLTrustStorePath, required=false}");
    }
}
