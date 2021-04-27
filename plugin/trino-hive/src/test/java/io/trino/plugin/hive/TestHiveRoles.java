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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHiveRoles
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.create();
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod()
    {
        for (String role : listRoles()) {
            executeFromAdmin("DROP ROLE " + role);
        }
    }

    @Test
    public void testCreateRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        assertEquals(listRoles(), ImmutableSet.of("role1"));
        assertEquals(listRoles(), ImmutableSet.of("role1"));
    }

    @Test
    public void testCreateDuplicateRole()
    {
        executeFromAdmin("CREATE ROLE duplicate_role");
        assertQueryFails(createAdminSession(), "CREATE ROLE duplicate_role", ".*?Role 'duplicate_role' already exists");
    }

    @Test
    public void testCreateRoleWithAdminOption()
    {
        assertQueryFails(createAdminSession(), "CREATE ROLE role1 WITH ADMIN admin", ".*?Hive Connector does not support WITH ADMIN statement");
    }

    @Test
    public void testCreateReservedRole()
    {
        assertQueryFails(createAdminSession(), "CREATE ROLE all", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
        assertQueryFails(createAdminSession(), "CREATE ROLE default", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
        assertQueryFails(createAdminSession(), "CREATE ROLE none", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
        assertQueryFails(createAdminSession(), "CREATE ROLE None", "Role name cannot be one of the reserved roles: \\[all, default, none\\]");
    }

    @Test
    public void testCreateRoleByNonAdminUser()
    {
        assertQueryFails(createUserSession("non_admin_user"), "CREATE ROLE role1", "Access Denied: Cannot create role role1");
    }

    @Test
    public void testDropRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        assertEquals(listRoles(), ImmutableSet.of("role1"));
        executeFromAdmin("DROP ROLE role1");
        assertEquals(listRoles(), ImmutableSet.of());
    }

    @Test
    public void testDropNonExistentRole()
    {
        assertQueryFails(createAdminSession(), "DROP ROLE non_existent_role", ".*?Role 'non_existent_role' does not exist");
    }

    @Test
    public void testDropRoleByNonAdminUser()
    {
        assertQueryFails(createUserSession("non_admin_user"), "DROP ROLE role1", "Access Denied: Cannot drop role role1");
    }

    @Test
    public void testListRolesByNonAdminUser()
    {
        assertQueryFails(createUserSession("non_admin_user"), "SELECT * FROM hive.information_schema.roles", "Access Denied: Cannot select from table information_schema.roles");
    }

    @Test
    public void testPublicRoleIsGrantedToAnyone()
    {
        assertContains(listApplicableRoles("some_user"), applicableRoles("some_user", "USER", "public", "NO"));
    }

    @Test
    public void testAdminRoleIsGrantedToAdmin()
    {
        assertContains(listApplicableRoles("admin"), applicableRoles("admin", "USER", "admin", "YES"));
    }

    @Test
    public void testGrantRoleToUser()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("GRANT role1 TO USER user");
        assertContains(listApplicableRoles("user"), applicableRoles("user", "USER", "role1", "NO"));
    }

    @Test
    public void testGrantRoleToRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO"));
    }

    @Test
    public void testGrantRoleWithAdminOption()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user WITH ADMIN OPTION");
        executeFromAdmin("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "YES",
                "role1", "ROLE", "role2", "YES"));
    }

    @Test
    public void testGrantRoleMultipleTimes()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        executeFromAdmin("GRANT role1 TO USER user WITH ADMIN OPTION");
        executeFromAdmin("GRANT role1 TO USER user WITH ADMIN OPTION");
        executeFromAdmin("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        executeFromAdmin("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "YES",
                "role1", "ROLE", "role2", "YES"));
    }

    @Test
    public void testGrantNonExistingRole()
    {
        assertQueryFails("GRANT grant_revoke_role_existing_1 TO USER grant_revoke_existing_user_1", ".*?Role 'grant_revoke_role_existing_1' does not exist");
        executeFromAdmin("CREATE ROLE grant_revoke_role_existing_1");
        assertQueryFails("GRANT grant_revoke_role_existing_1 TO ROLE grant_revoke_role_existing_2", ".*?Role 'grant_revoke_role_existing_2' does not exist");
    }

    @Test
    public void testRevokeRoleFromUser()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("GRANT role1 TO USER user");
        assertContains(listApplicableRoles("user"), applicableRoles("user", "USER", "role1", "NO"));

        executeFromAdmin("REVOKE role1 FROM USER user");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles("user", "USER", "public", "NO"));
    }

    @Test
    public void testRevokeRoleFromRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO"));

        executeFromAdmin("REVOKE role2 FROM ROLE role1");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "public", "NO",
                "user", "USER", "role1", "NO"));
    }

    @Test
    public void testDropGrantedRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("GRANT role1 TO USER user");
        assertContains(listApplicableRoles("user"), applicableRoles("user", "USER", "role1", "NO"));

        executeFromAdmin("DROP ROLE role1");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles("user", "USER", "public", "NO"));
    }

    @Test
    public void testRevokeTransitiveRoleFromUser()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("CREATE ROLE role3");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        executeFromAdmin("GRANT role3 TO ROLE role2");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO",
                "role2", "ROLE", "role3", "NO"));

        executeFromAdmin("REVOKE role1 FROM USER user");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles("user", "USER", "public", "NO"));
    }

    @Test
    public void testRevokeTransitiveRoleFromRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("CREATE ROLE role3");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        executeFromAdmin("GRANT role3 TO ROLE role2");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO",
                "role2", "ROLE", "role3", "NO"));

        executeFromAdmin("REVOKE role2 FROM ROLE role1");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "public", "NO",
                "user", "USER", "role1", "NO"));
    }

    @Test
    public void testDropTransitiveRole()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("CREATE ROLE role3");
        executeFromAdmin("GRANT role1 TO USER user");
        executeFromAdmin("GRANT role2 TO ROLE role1");
        executeFromAdmin("GRANT role3 TO ROLE role2");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO",
                "role2", "ROLE", "role3", "NO"));

        executeFromAdmin("DROP ROLE role2");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "public", "NO",
                "user", "USER", "role1", "NO"));
    }

    @Test
    public void testRevokeAdminOption()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user WITH ADMIN OPTION");
        executeFromAdmin("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "YES",
                "role1", "ROLE", "role2", "YES"));

        executeFromAdmin("REVOKE ADMIN OPTION FOR role1 FROM USER user");
        executeFromAdmin("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO"));
    }

    @Test
    public void testRevokeRoleMultipleTimes()
    {
        executeFromAdmin("CREATE ROLE role1");
        executeFromAdmin("CREATE ROLE role2");
        executeFromAdmin("GRANT role1 TO USER user WITH ADMIN OPTION");
        executeFromAdmin("GRANT role2 TO ROLE role1 WITH ADMIN OPTION");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "YES",
                "role1", "ROLE", "role2", "YES"));

        executeFromAdmin("REVOKE ADMIN OPTION FOR role1 FROM USER user");
        executeFromAdmin("REVOKE ADMIN OPTION FOR role1 FROM USER user");
        executeFromAdmin("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");
        executeFromAdmin("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1");
        assertContains(listApplicableRoles("user"), applicableRoles(
                "user", "USER", "role1", "NO",
                "role1", "ROLE", "role2", "NO"));

        executeFromAdmin("REVOKE role1 FROM USER user");
        executeFromAdmin("REVOKE role1 FROM USER user");
        executeFromAdmin("REVOKE role2 FROM ROLE role1");
        executeFromAdmin("REVOKE role2 FROM ROLE role1");
        assertEqualsIgnoreOrder(listApplicableRoles("user"), applicableRoles("user", "USER", "public", "NO"));
    }

    @Test
    public void testRevokeNonExistingRole()
    {
        assertQueryFails(createAdminSession(), "REVOKE grant_revoke_role_existing_1 FROM USER grant_revoke_existing_user_1", ".*?Role 'grant_revoke_role_existing_1' does not exist");
        executeFromAdmin("CREATE ROLE grant_revoke_role_existing_1");
        assertQueryFails(createAdminSession(), "REVOKE grant_revoke_role_existing_1 FROM ROLE grant_revoke_role_existing_2", ".*?Role 'grant_revoke_role_existing_2' does not exist");
    }

    @Test
    public void testSetRole()
    {
        executeFromAdmin("CREATE ROLE set_role_1");
        executeFromAdmin("CREATE ROLE set_role_2");
        executeFromAdmin("CREATE ROLE set_role_3");
        executeFromAdmin("CREATE ROLE set_role_4");
        executeFromAdmin("GRANT set_role_1 TO USER set_user_1");
        executeFromAdmin("GRANT set_role_2 TO ROLE set_role_1");
        executeFromAdmin("GRANT set_role_3 TO ROLE set_role_2");

        Session unsetRole = Session.builder(getSession())
                .setIdentity(Identity.ofUser("set_user_1"))
                .build();
        Session setRoleAll = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.ALL, Optional.empty())).build())
                .build();
        Session setRoleNone = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.NONE, Optional.empty())).build())
                .build();
        Session setRole1 = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("set_role_1"))).build())
                .build();
        Session setRole2 = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("set_role_2"))).build())
                .build();
        Session setRole3 = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("set_role_3"))).build())
                .build();
        Session setRole4 = Session.builder(getSession())
                .setIdentity(Identity.forUser("set_user_1").withRole("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("set_role_4"))).build())
                .build();

        MaterializedResult actual = getQueryRunner().execute(unsetRole, "SELECT * FROM hive.information_schema.applicable_roles");
        MaterializedResult expected = MaterializedResult.resultBuilder(unsetRole, createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("set_user_1", "USER", "public", "NO")
                .row("set_user_1", "USER", "set_role_1", "NO")
                .row("set_role_1", "ROLE", "set_role_2", "NO")
                .row("set_role_2", "ROLE", "set_role_3", "NO")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(unsetRole, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(unsetRole, createUnboundedVarcharType())
                .row("public")
                .row("set_role_1")
                .row("set_role_2")
                .row("set_role_3")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(setRoleAll, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(setRoleAll, createUnboundedVarcharType())
                .row("public")
                .row("set_role_1")
                .row("set_role_2")
                .row("set_role_3")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(setRoleNone, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(setRoleNone, createUnboundedVarcharType())
                .row("public")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(setRole1, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(setRole1, createUnboundedVarcharType())
                .row("public")
                .row("set_role_1")
                .row("set_role_2")
                .row("set_role_3")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(setRole2, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(setRole2, createUnboundedVarcharType())
                .row("public")
                .row("set_role_2")
                .row("set_role_3")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        actual = getQueryRunner().execute(setRole3, "SELECT * FROM hive.information_schema.enabled_roles");
        expected = MaterializedResult.resultBuilder(setRole3, createUnboundedVarcharType())
                .row("public")
                .row("set_role_3")
                .build();
        assertEqualsIgnoreOrder(actual, expected);

        assertQueryFails(setRole4, "SELECT * FROM hive.information_schema.enabled_roles", ".*?Cannot set role set_role_4");

        executeFromAdmin("DROP ROLE set_role_1");
        executeFromAdmin("DROP ROLE set_role_2");
        executeFromAdmin("DROP ROLE set_role_3");
        executeFromAdmin("DROP ROLE set_role_4");
    }

    private Set<String> listRoles()
    {
        return executeFromAdmin("SELECT * FROM hive.information_schema.roles")
                .getMaterializedRows()
                .stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toSet());
    }

    private MaterializedResult listApplicableRoles(String user)
    {
        return executeFromUser(user, "SELECT * FROM hive.information_schema.applicable_roles");
    }

    private MaterializedResult applicableRoles(String... values)
    {
        List<Type> types = ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType());
        int rowLength = types.size();
        checkArgument(values.length % rowLength == 0);
        MaterializedResult.Builder result = MaterializedResult.resultBuilder(getSession(), types);
        Object[] row = null;
        for (int i = 0; i < values.length; i++) {
            if (i % rowLength == 0) {
                if (row != null) {
                    result.row(row);
                }
                row = new Object[rowLength];
            }
            checkState(row != null);
            row[i % rowLength] = values[i];
        }
        if (row != null) {
            result.row(row);
        }
        return result.build();
    }

    private MaterializedResult executeFromAdmin(String sql)
    {
        return getQueryRunner().execute(createAdminSession(), sql);
    }

    private MaterializedResult executeFromUser(String user, String sql)
    {
        return getQueryRunner().execute(createUserSession(user), sql);
    }

    private Session createAdminSession()
    {
        return Session.builder(getSession())
                .setIdentity(Identity.forUser("admin").withRole("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("admin"))).build())
                .build();
    }

    private Session createUserSession(String user)
    {
        return Session.builder(getSession())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
