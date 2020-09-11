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
package io.prestosql.tests.product.launcher.suite;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;
import io.prestosql.tests.product.launcher.env.Environments;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

public class SuiteTestRun
{
    private static final String SKIP_TEST_ARGUMENT = "DISTRO_SKIP_TEST";
    private static final String SKIP_GROUP_ARGUMENT = "DISTRO_SKIP_GROUP";

    private static final String TEMPTO_TEST_ARG = "-t";
    private static final String TEMPTO_GROUP_ARG = "-g";
    private static final String TEMPTO_EXCLUDE_GROUP_ARG = "-x";
    private static final String TEMPTO_EXCLUDE_TEST_ARG = "-e";

    private final Class<? extends EnvironmentProvider> environment;
    private final List<String> groups;
    private final List<String> excludedGroups;
    private final List<String> tests;
    private final List<String> excludedTests;

    public SuiteTestRun(Class<? extends EnvironmentProvider> environment, List<String> groups, List<String> excludedGroups, List<String> tests, List<String> excludedTests)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.groups = requireNonNull(groups, "groups is null");
        this.excludedGroups = requireNonNull(excludedGroups, "excludedGroups is null");
        this.tests = requireNonNull(tests, "tests is null");
        this.excludedTests = requireNonNull(excludedTests, "excludedTests is null");
    }

    public Class<? extends EnvironmentProvider> getEnvironment()
    {
        return environment;
    }

    public String getEnvironmentName()
    {
        return Environments.nameForClass(environment);
    }

    public List<String> getGroups()
    {
        return groups;
    }

    public List<String> getExcludedGroups()
    {
        return ImmutableList.<String>builder()
                .addAll(excludedGroups)
                .addAll(splitValueFromEnv(SKIP_GROUP_ARGUMENT))
                .build();
    }

    public List<String> getTests()
    {
        return tests;
    }

    public List<String> getExcludedTests()
    {
        return ImmutableList.<String>builder()
                .addAll(excludedTests)
                .addAll(splitValueFromEnv(SKIP_TEST_ARGUMENT))
                .build();
    }

    public List<String> getTemptoRunArguments(EnvironmentConfig environmentConfig)
    {
        ImmutableList.Builder<String> arguments = ImmutableList.builder();
        Joiner joiner = Joiner.on(",");

        if (!groups.isEmpty()) {
            arguments.add(TEMPTO_GROUP_ARG, joiner.join(groups));
        }

        Iterable<String> excludedGroups = Iterables.concat(getExcludedGroups(), environmentConfig.getExcludedGroups());
        if (!Iterables.isEmpty(excludedGroups)) {
            arguments.add(TEMPTO_EXCLUDE_GROUP_ARG, joiner.join(excludedGroups));
        }

        if (!tests.isEmpty()) {
            arguments.add(TEMPTO_TEST_ARG, joiner.join(tests));
        }

        Iterable<String> excludedTests = Iterables.concat(getExcludedTests(), environmentConfig.getExcludedTests());
        if (!Iterables.isEmpty(excludedTests)) {
            arguments.add(TEMPTO_EXCLUDE_TEST_ARG, joiner.join(excludedTests));
        }

        return arguments.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("environment", getEnvironmentName())
                .add("groups", getGroups())
                .add("excludedGroups", getExcludedGroups())
                .add("tests", getTests())
                .add("excludedTests", getExcludedTests())
                .toString();
    }

    private static List<String> splitValueFromEnv(String key)
    {
        String value = getenv(key);

        if (Strings.isNullOrEmpty(value)) {
            return ImmutableList.of();
        }

        return Splitter.on(',').trimResults().omitEmptyStrings().splitToList(value);
    }

    public static Builder testOnEnvironment(Class<? extends EnvironmentProvider> environment)
    {
        return new Builder(environment);
    }

    public static class Builder
    {
        private Class<? extends EnvironmentProvider> environment;
        private List<String> groups = ImmutableList.of();
        private List<String> excludedGroups = ImmutableList.of();
        private List<String> excludedTests = ImmutableList.of();
        private List<String> tests = ImmutableList.of();

        private Builder(Class<? extends EnvironmentProvider> environment)
        {
            this.environment = requireNonNull(environment, "environment is null");
        }

        public Builder withGroups(String... groups)
        {
            this.groups = ImmutableList.copyOf(groups);
            return this;
        }

        public Builder withExcludedGroups(String... groups)
        {
            this.excludedGroups = ImmutableList.copyOf(groups);
            return this;
        }

        public Builder withTests(String... tests)
        {
            this.tests = ImmutableList.copyOf(tests);
            return this;
        }

        public Builder withExcludedTests(String... tests)
        {
            this.excludedTests = ImmutableList.copyOf(tests);
            return this;
        }

        public SuiteTestRun build()
        {
            return new SuiteTestRun(environment, groups, excludedGroups, tests, excludedTests);
        }
    }
}
