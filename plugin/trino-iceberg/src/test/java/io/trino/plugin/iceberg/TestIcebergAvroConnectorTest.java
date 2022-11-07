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
package io.trino.plugin.iceberg;

import io.trino.Session;
import org.testng.SkipException;

import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;

public class TestIcebergAvroConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergAvroConnectorTest()
    {
        super(AVRO);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return false;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return false;
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return session;
    }

    @Override
    public void testIncorrectIcebergFileSizes()
    {
        throw new SkipException("Avro does not do tail reads");
    }
}
