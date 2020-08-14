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
package io.prestosql.plugin.password.salesforce;

import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.prestosql.spi.security.AccessDeniedException;
import org.testng.SkipException;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.security.Principal;

import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSalesforceBasicAuthenticator
{
    private HttpClient testHttpClient;
    private String org;
    private String username;
    private String password;
    private String successResponse;
    private String failedResponse;
    private SalesforceConfig config;
    private boolean forReal;

    @BeforeSuite
    void initOnce()
    {
        successResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns=\"urn:partner.soap.sforce.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><soapenv:Body><loginResponse><result><metadataServerUrl>https://example.salesforce.com/services/Soap/m/46.0/example</metadataServerUrl><passwordExpired>false</passwordExpired><sandbox>false</sandbox><serverUrl>https://example.salesforce.com/services/Soap/u/46.0/example</serverUrl><sessionId>example</sessionId><userId>example</userId><userInfo><accessibilityMode>false</accessibilityMode><chatterExternal>false</chatterExternal><currencySymbol>$</currencySymbol><orgAttachmentFileSizeLimit>5242880</orgAttachmentFileSizeLimit><orgDefaultCurrencyIsoCode>USD</orgDefaultCurrencyIsoCode><orgDefaultCurrencyLocale>en_US</orgDefaultCurrencyLocale><orgDisallowHtmlAttachments>false</orgDisallowHtmlAttachments><orgHasPersonAccounts>true</orgHasPersonAccounts><organizationId>%s</organizationId><organizationMultiCurrency>false</organizationMultiCurrency><organizationName>example</organizationName><profileId>example</profileId><roleId>example</roleId><sessionSecondsValid>7200</sessionSecondsValid><userDefaultCurrencyIsoCode xsi:nil=\"true\"/><userEmail>user@salesforce.com</userEmail><userFullName>Vince Chase</userFullName><userId>example</userId><userLanguage>en_US</userLanguage><userLocale>en_US</userLocale><userName>%s</userName><userTimeZone>America/Chicago</userTimeZone><userType>Standard</userType><userUiSkin>Theme3</userUiSkin></userInfo></result></loginResponse></soapenv:Body></soapenv:Envelope>";
        failedResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:sf=\"urn:fault.partner.soap.sforce.com\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><soapenv:Body><soapenv:Fault><faultcode>sf:INVALID_LOGIN</faultcode><faultstring>INVALID_LOGIN: Invalid username, password, security token; or user locked out.</faultstring><detail><sf:LoginFault xsi:type=\"sf:LoginFault\"><sf:exceptionCode>INVALID_LOGIN</sf:exceptionCode><sf:exceptionMessage>Invalid username, password, security token; or user locked out.</sf:exceptionMessage></sf:LoginFault></detail></soapenv:Fault></soapenv:Body></soapenv:Envelope>";
        forReal = false;
        String forRealEnvVar = System.getenv("SALESFORCE_TEST_FORREAL");
        if (forRealEnvVar != null && forRealEnvVar.equalsIgnoreCase("TRUE")) {
            forReal = true;
        }
    }

    @Test
    public void createAuthenticatedPrincipal_success()
            throws InterruptedException
    {
        org = "my18CharOrgId";  // As if from salesforce.org property.
        username = "user@salesforce.com";
        password = "passtoken";

        config = new SalesforceConfig()
                .setOrgs(org)
                .setCacheExpireSeconds(1); // Test cache timeout.

        String xmlResponse = String.format(successResponse, org, username);

        testHttpClient = new TestingHttpClient((request -> mockResponse(HttpStatus.OK, MediaType.ANY_TEXT_TYPE, xmlResponse)));
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test principal name.");

        principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test principal name from cache.");

        Thread.sleep(2000L);
        principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test principal name from expired cache.");
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void createAuthenticatedPrincipal_wrongOrg()
    {
        org = "my18CharOrgId";  // As if from salesforce.org property.
        username = "user@salesforce.com";
        password = "passtoken";

        config = new SalesforceConfig()
                .setOrgs(org);

        final String xmlResponse = String.format(successResponse, "NotMyOrg", username);

        testHttpClient = new TestingHttpClient((request -> mockResponse(HttpStatus.OK, MediaType.ANY_TEXT_TYPE, xmlResponse)));
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);
        authenticator.createAuthenticatedPrincipal(username, password);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void createAuthenticatedPrincipal_badPass()
    {
        org = "my18CharOrgId";  // As if from salesforce.org property.
        username = "user@salesforce.com";
        password = "passtoken";

        config = new SalesforceConfig()
                .setOrgs(org);

        final String xmlResponse = failedResponse;

        testHttpClient = new TestingHttpClient((request -> mockResponse(HttpStatus.INTERNAL_SERVER_ERROR, MediaType.ANY_TEXT_TYPE, xmlResponse)));
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);
        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
    }

    @Test
    public void createAuthenticatedPrincipalAllOrgs()
    {
        org = "all";  // As if from salesforce.org property.
        username = "user@salesforce.com";
        password = "passtoken";

        config = new SalesforceConfig()
                .setOrgs(org);

        String xmlResponse = String.format(successResponse, "some18CharOrgId", username);

        testHttpClient = new TestingHttpClient((request -> mockResponse(HttpStatus.OK, MediaType.ANY_TEXT_TYPE, xmlResponse)));
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test allowing all orgs.");
    }

    @Test
    public void createAuthenticatedPrincipalFewOrgs()
    {
        org = "my18CharOrgId,your18CharOrgId, his18CharOrgId ,her18CharOrgId";  // As if from salesforce.org property.
        username = "user@salesforce.com";
        password = "passtoken";

        config = new SalesforceConfig()
                .setOrgs(org);

        String xmlResponse = String.format(successResponse, "my18CharOrgId", username);

        testHttpClient = new TestingHttpClient((request -> mockResponse(HttpStatus.OK, MediaType.ANY_TEXT_TYPE, xmlResponse)));
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test allowing a few orgs.");
    }

    /*
     * Real tests that use Salesforce credentials and actually attempt to login.
     * These should be disabled for automated builds and test runs.
     *
     * In order to run these, the following environment variables need to be set.
     *
     *   - SALESFORCE_TEST_ORG (this is the 18 character organization id)
     *   - SALESFORCE_TEST_USERNAME
     *   - SALESFORCE_TEST_PASSWORD (this must be password and security token concatenation)
     *   - SALESFORCE_TEST_FORREAL must be TRUE
     */

    // Test a real login.
    @Test(description = "Test principal name for real, yo!")
    void createAuthenticatedPrincipal_realSuccess()
    {
        // Skip this test if SALESFORCE_TEST_FORREAL is not set to TRUE.
        if (!forReal) {
            throw new SkipException("Skipping real tests.");
        }

        org = System.getenv("SALESFORCE_TEST_ORG");
        if (org == null || org.length() == 0) {
            fail("Must set SALESFORCE_TEST_ORG environment variable.");
        }
        username = System.getenv("SALESFORCE_TEST_USERNAME");
        password = System.getenv("SALESFORCE_TEST_PASSWORD");
        if (username == null || username.length() == 0 || password == null || password.length() == 0) {
            fail("Must set SALESFORCE_TEST_USERNAME and SALESFORCE_TEST_PASSWORD environment variables.");
        }

        config = new SalesforceConfig()
                .setOrgs(org);
        testHttpClient = new JettyHttpClient();
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test principal name for real, yo!");
    }

    // Test a real login for a different org.
    @Test(expectedExceptions = AccessDeniedException.class, description = "Test got wrong org for real, yo!")
    void createAuthenticatedPrincipal_realWrongOrg()
    {
        // Skip this test if SALESFORCE_TEST_FORREAL is not set to TRUE.
        if (!forReal) {
            throw new SkipException("Skipping real tests.");
        }

        username = System.getenv("SALESFORCE_TEST_USERNAME");
        password = System.getenv("SALESFORCE_TEST_PASSWORD");
        if (username == null || username.length() == 0 || password == null || password.length() == 0) {
            fail("Must set SALESFORCE_TEST_USERNAME and SALESFORCE_TEST_PASSWORD environment variables.");
        }

        org = "NotMyOrg";
        config = new SalesforceConfig()
                .setOrgs(org);
        testHttpClient = new JettyHttpClient();
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        authenticator.createAuthenticatedPrincipal(username, password);
    }

    // Test a real login for a different org.
    @Test
    void createAuthenticatedPrincipal_realAllOrgs()
    {
        // Skip this test if SALESFORCE_TEST_FORREAL is not set to TRUE.
        if (!forReal) {
            throw new SkipException("Skipping real tests.");
        }

        username = System.getenv("SALESFORCE_TEST_USERNAME");
        password = System.getenv("SALESFORCE_TEST_PASSWORD");
        if (username == null || username.length() == 0 || password == null || password.length() == 0) {
            fail("Must set SALESFORCE_TEST_USERNAME and SALESFORCE_TEST_PASSWORD environment variables.");
        }

        config = new SalesforceConfig()
                .setOrgs("all");

        testHttpClient = new JettyHttpClient();
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);

        Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
        assertEquals(principal.getName(), username, "Test no org check for real, yo!");
    }

    // Test a login with a bad password.
    @Test(expectedExceptions = AccessDeniedException.class, description = "Test bad password for real, yo!")
    void createAuthenticatedPrincipal_realBadPassword()
    {
        // Skip this test if SALESFORCE_TEST_FORREAL is not set to TRUE.
        if (!forReal) {
            throw new SkipException("Skipping real tests.");
        }

        org = System.getenv("SALESFORCE_TEST_ORG");
        if (org == null || org.length() == 0) {
            fail("Must set SALESFORCE_TEST_ORG environment variable.");
        }
        username = System.getenv("SALESFORCE_TEST_USERNAME");
        password = System.getenv("SALESFORCE_TEST_PASSWORD");
        if (username == null || username.length() == 0 || password == null || password.length() == 0) {
            fail("Must set SALESFORCE_TEST_USERNAME and SALESFORCE_TEST_PASSWORD environment variables.");
        }

        config = new SalesforceConfig()
                .setOrgs(org);
        testHttpClient = new JettyHttpClient();
        SalesforceBasicAuthenticator authenticator = new SalesforceBasicAuthenticator(config, testHttpClient);
        authenticator.createAuthenticatedPrincipal(username, "NotMyPassword");
    }
}
