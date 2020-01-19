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
package io.prestosql.plugin.salesforce.driver.connection;

public class ForceConnectionInfo
{
    private String userName;
    private String password;
    private String sessionId;
    private Boolean sandbox;
    private String securityToken;

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(String sessionId)
    {
        this.sessionId = sessionId;
    }

    public Boolean getSandbox()
    {
        return sandbox;
    }

    public void setSandbox(Boolean sandbox)
    {
        this.sandbox = sandbox;
    }

    public void setSecurityToken(String securityToken)
    {
        this.securityToken = securityToken;
    }

    public String getSecurityToken()
    {
        return securityToken;
    }
}
