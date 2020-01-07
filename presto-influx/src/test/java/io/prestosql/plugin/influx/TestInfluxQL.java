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

package io.prestosql.plugin.influx;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestInfluxQL
{
    @Test
    public void test()
    {
        InfluxQL test = new InfluxQL();

        test.addIdentifier("hello").append(" = ").add("world");
        assertEquals(test.toString(), "hello = 'world'");

        test.truncate(0);

        test.addIdentifier("åäö").append(" = ").add("\"åäö'");
        assertEquals(test.toString(), "\"åäö\" = '\"åäö\\''");

        test.truncate(0);

        // from is a reserved word so must be quoted
        test.addIdentifier("frOm").append(" = ").add("to");
        assertEquals(test.toString(), "\"frOm\" = 'to'");
    }
}
