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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPartitionedOutputBufferManager
{
    @Test
    public void test()
    {
        AtomicReference<OutputBuffers> outputBufferTarget = new AtomicReference<>();

        PartitionedOutputBufferManager hashOutputBufferManager = new PartitionedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 4, outputBufferTarget::set);

        // output buffers are set immediately when the manager is created
        assertOutputBuffers(outputBufferTarget.get());

        // add buffers, which does not cause an error
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(0)), false);
        assertOutputBuffers(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(3)), true);
        assertOutputBuffers(outputBufferTarget.get());

        // try to a buffer out side of the partition range, which should result in an error
        assertThatThrownBy(() -> hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(5)), false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected new output buffer 5");
        assertOutputBuffers(outputBufferTarget.get());

        // try to a buffer out side of the partition range, which should result in an error
        assertThatThrownBy(() -> hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(6)), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected new output buffer 6");
        assertOutputBuffers(outputBufferTarget.get());
    }

    private static void assertOutputBuffers(OutputBuffers outputBuffers)
    {
        assertNotNull(outputBuffers);
        assertTrue(outputBuffers.getVersion() > 0);
        assertTrue(outputBuffers.isNoMoreBufferIds());
        Map<OutputBufferId, Integer> buffers = outputBuffers.getBuffers();
        assertEquals(buffers.size(), 4);
        for (int partition = 0; partition < 4; partition++) {
            assertEquals(buffers.get(new OutputBufferId(partition)), Integer.valueOf(partition));
        }
    }
}
