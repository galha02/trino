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
package io.trino.type;

import static io.trino.type.UnknownType.UNKNOWN;

public class TestUnknownType
        extends AbstractTestType
{
    public TestUnknownType()
    {
        super(UNKNOWN,
                boolean.class,
                UNKNOWN.createBlockBuilder(null, 3)
                        .appendNull()
                        .appendNull()
                        .appendNull()
                        .build());
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testPreviousValue()
    {
        // There is no value of this type, so getPreviousValue() cannot be invoked
    }

    @Override
    public void testNextValue()
    {
        // There is no value of this type, so getNextValue() cannot be invoked
    }
}
