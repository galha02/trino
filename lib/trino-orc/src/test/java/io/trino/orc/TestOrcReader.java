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
package io.trino.orc;

import org.testng.annotations.Test;

import static io.prestosql.orc.OrcTester.fullOrcTester;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

@Test
public class TestOrcReader
        extends AbstractTestOrcReader
{
    public TestOrcReader()
    {
        super(OrcTester.quickOrcTester());
    }

    @Test
    public void testDoubleSequenceFull()
            throws Exception
    {
        // run a single test using the full tester
        fullOrcTester().testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }
}
