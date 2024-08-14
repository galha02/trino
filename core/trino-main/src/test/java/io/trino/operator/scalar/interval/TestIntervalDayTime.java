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
package io.trino.operator.scalar.interval;

import io.trino.sql.query.QueryAssertions;
import io.trino.type.SqlIntervalDayTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalDayTime
{
    protected QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLiterals()
    {
        assertThat(assertions.expression("INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '12 10:45:32.12' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '12 10:45:32' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO SECOND"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO MINUTE"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO HOUR"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO HOUR"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '30' DAY"))
                .isEqualTo(interval(30, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '90' DAY"))
                .isEqualTo(interval(90, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45:32.123' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '10:45:32.12' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '10:45:32' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '45:32.123' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '45:32.12' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '45:32' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '32.123' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 123));

        assertThat(assertions.expression("INTERVAL '32.12' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 120));

        assertThat(assertions.expression("INTERVAL '32' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 0));

        assertThatThrownBy(assertions.expression("INTERVAL '12X' DAY")::evaluate)
                .hasMessage("line 1:12: '12X' is not a valid INTERVAL literal");

        assertThatThrownBy(assertions.expression("INTERVAL '12 10' DAY")::evaluate)
                .hasMessage("line 1:12: '12 10' is not a valid INTERVAL literal");

        assertThatThrownBy(assertions.expression("INTERVAL '12 X' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: '12 X' is not a valid INTERVAL literal");

        assertThatThrownBy(assertions.expression("INTERVAL '12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: '12 -10' is not a valid INTERVAL literal");

        assertThatThrownBy(assertions.expression("INTERVAL '--12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: '--12 -10' is not a valid INTERVAL literal");

        assertThatThrownBy(assertions.expression("INTERVAL '12' DAY TO YEAR")::evaluate)
                .hasMessage("line 1:33: mismatched input 'YEAR'. Expecting: 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12' DAY TO MONTH")::evaluate)
                .hasMessage("line 1:33: mismatched input 'MONTH'. Expecting: 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12' DAY TO DAY")::evaluate)
                .hasMessage("line 1:33: mismatched input 'DAY'. Expecting: 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12' HOUR TO HOUR")::evaluate)
                .hasMessage("line 1:34: mismatched input 'HOUR'. Expecting: 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12' MINUTE TO MINUTE")::evaluate)
                .hasMessage("line 1:36: mismatched input 'MINUTE'. Expecting: 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12' SECOND TO SECOND")::evaluate)
                .hasMessageContaining("line 1:33: mismatched input 'TO'.");
    }

    private static SqlIntervalDayTime interval(int day, int hour, int minute, int second, int milliseconds)
    {
        return new SqlIntervalDayTime(day, hour, minute, second, milliseconds);
    }
}
