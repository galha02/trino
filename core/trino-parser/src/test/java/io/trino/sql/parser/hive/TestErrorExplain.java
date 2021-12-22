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
package io.trino.sql.parser.hive;

import io.trino.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class TestErrorExplain
        extends SQLTester
{
    @Test
    public void test01()
    {
        String sql = "select a from t where m 'sdffs'";
        try {
            runHiveSQL(sql);
        }
        catch (ParsingException e) {
        }
    }

    @Test
    public void test02()
    {
        String sql = "select a from t where between 'sdffs' and 'sad'";
        try {
            runHiveSQL(sql);
        }
        catch (ParsingException e) {
        }
    }

    @Test
    public void test03()
    {
        String sql = "select a from t where like 'sdffs'";
        try {
            runHiveSQL(sql);
        }
        catch (ParsingException e) {
        }
    }

    @Test
    public void testNested01()
    {
        String hiveSql = "select user FROM ((select user from tbl))";
        try {
            runHiveSQL(hiveSql);
        }
        catch (ParsingException e) {
        }
    }

    @Test
    public void testNested02()
    {
        String hiveSql = "select user FROM ((select user,id from tbl left join tbl1))";
        try {
            runHiveSQL(hiveSql);
        }
        catch (ParsingException e) {
        }
    }
}
