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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.spi.HostAddress;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PhoenixSplit
        extends JdbcSplit
{
    private final List<HostAddress> addresses;
    private final WrappedPhoenixInputSplit phoenixInputSplit;

    @JsonCreator
    public PhoenixSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("phoenixInputSplit") WrappedPhoenixInputSplit wrappedPhoenixInputSplit)
    {
        super(Optional.empty());
        requireNonNull(wrappedPhoenixInputSplit, "wrappedPhoenixInputSplit is null");
        requireNonNull(addresses, "addresses is null");
        this.addresses = addresses;
        this.phoenixInputSplit = wrappedPhoenixInputSplit;
    }

    @JsonProperty("phoenixInputSplit")
    public WrappedPhoenixInputSplit getWrappedPhoenixInputSplit()
    {
        return phoenixInputSplit;
    }

    @JsonIgnore
    public PhoenixInputSplit getPhoenixInputSplit()
    {
        return phoenixInputSplit.getPhoenixInputSplit();
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }
}
