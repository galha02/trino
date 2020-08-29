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
package io.prestosql.tests.product.launcher.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentConfigFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteFactory;
import io.prestosql.tests.product.launcher.suite.SuiteModule;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import javax.inject.Inject;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.env.EnvironmentOptions.applySuiteConfig;
import static java.lang.String.format;
import static java.lang.System.exit;
import static java.util.Objects.requireNonNull;

@Command(name = "run", description = "run suite tests")
public class SuiteRun
        implements Runnable
{
    private static final Logger log = Logger.get(SuiteRun.class);

    private final Module additionalEnvironments;
    private final Module additionalSuites;

    @Inject
    public SuiteRun.SuiteRunOptions suiteRunOptions = new SuiteRun.SuiteRunOptions();

    @Inject
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    public SuiteRun(Extensions extensions)
    {
        this.additionalEnvironments = requireNonNull(extensions, "extensions is null").getAdditionalEnvironments();
        this.additionalSuites = requireNonNull(extensions, "extensions is null").getAdditionalSuites();
    }

    @Override
    public void run()
    {
        Module environmentModule = new EnvironmentModule(additionalEnvironments);
        EnvironmentConfig config = getEnvironmentConfig(environmentModule, suiteRunOptions.config);

        runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new SuiteModule(additionalSuites))
                        .add(environmentModule)
                        .add(suiteRunOptions.toModule())
                        .add(applySuiteConfig(environmentOptions, config).toModule())
                        .build(),
                SuiteRun.Execution.class);
    }

    private static EnvironmentConfig getEnvironmentConfig(Module environmentModule, String configName)
    {
        Injector injector = Guice.createInjector(environmentModule);
        EnvironmentConfigFactory instance = injector.getInstance(EnvironmentConfigFactory.class);
        return instance.getConfig(configName);
    }

    public static class SuiteRunOptions
    {
        @Option(name = "--suite", title = "suite", description = "the name of the suite to run", required = true)
        public String suite;

        @Option(name = "--config", title = "config", description = "the name of the environment config to use")
        public String config = "config-default";

        @Option(name = "--test-jar", title = "test jar", description = "path to test jar")
        public File testJar = new File("presto-product-tests/target/presto-product-tests-${project.version}-executable.jar");

        public Module toModule()
        {
            return binder -> binder.bind(SuiteRun.SuiteRunOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Runnable
    {
        private final SuiteRunOptions suiteRunOptions;
        private final EnvironmentOptions environmentOptions;
        private final PathResolver pathResolver;
        private final SuiteFactory suiteFactory;
        private final EnvironmentFactory environmentFactory;
        private final EnvironmentConfigFactory configFactory;

        @Inject
        public Execution(
                SuiteRunOptions suiteRunOptions,
                EnvironmentOptions environmentOptions,
                PathResolver pathResolver,
                SuiteFactory suiteFactory,
                EnvironmentFactory environmentFactory,
                EnvironmentConfigFactory configFactory)
        {
            this.suiteRunOptions = requireNonNull(suiteRunOptions, "suiteRunOptions is null");
            this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
        }

        @Override
        public void run()
        {
            String suiteName = requireNonNull(suiteRunOptions.suite, "suiteRunOptions.suite is null");

            Suite suite = suiteFactory.getSuite(suiteName);
            EnvironmentConfig environmentConfig = configFactory.getConfig(suiteRunOptions.config);
            List<SuiteTestRun> suiteTestRuns = suite.getTestRuns(environmentConfig);

            log.info("Starting suite '%s' with config '%s' and test runs: ", suiteName, environmentConfig.getConfigName());
            suiteTestRuns.forEach(job ->
                    log.info(" * environment '%s': groups: %s, excluded groups: %s, tests: %s, excluded tests: %s",
                            job.getEnvironmentName(), job.getGroups(), job.getExcludedGroups(), job.getTests(), job.getExcludedTests()));

            long suiteStartTime = System.nanoTime();
            ImmutableList.Builder<TestRunResult> results = ImmutableList.builder();

            for (SuiteTestRun testRun : suiteTestRuns) {
                results.add(executeSuiteTestRun(suiteName, testRun, environmentConfig));
            }

            List<TestRunResult> testRunsResults = results.build();
            boolean allSuccessful = testRunsResults.stream()
                    .allMatch(TestRunResult::isSuccessful);

            if (allSuccessful) {
                log.info("Suite succeeded in %s:", nanosSince(suiteStartTime));
                printTestRunsSummary(testRunsResults);
                exit(0);
            }

            log.error("Suite failed in %s:", nanosSince(suiteStartTime));
            printTestRunsSummary(testRunsResults);
            exit(1);
        }

        private static void printTestRunsSummary(List<TestRunResult> results)
        {
            long failedRuns = results.stream()
                    .filter(TestRunResult::hasFailed)
                    .count();

            log.info("Test runs summary (%d passed, %d failed): ", results.size() - failedRuns, failedRuns);
            results.forEach(result -> log.info(" * '%s' %s in %s", result.getSuiteRun(), result.isSuccessful() ? "PASSED" : "FAILED", result.getDuration()));
        }

        public TestRunResult executeSuiteTestRun(String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig)
        {
            log.info("Starting test run %s with config %s", suiteTestRun, environmentConfig);
            long startTime = System.nanoTime();

            Throwable t = null;
            try {
                TestRun.TestRunOptions testRunOptions = createTestRunOptions(suiteName, suiteTestRun, environmentConfig);
                log.info("Execute this test run using:\npresto-product-tests-launcher/bin/run-launcher test run %s", OptionsPrinter.format(environmentOptions, testRunOptions));
                new TestRun.Execution(environmentFactory, pathResolver, environmentOptions, testRunOptions, environmentConfig.extendEnvironment(suiteTestRun.getEnvironment())).run();
            }
            catch (Exception e) {
                t = e;
                log.error("Failed to execute test run %s", suiteTestRun, e);
            }

            return new TestRunResult(suiteTestRun, environmentConfig, nanosSince(startTime), Optional.ofNullable(t));
        }

        private TestRun.TestRunOptions createTestRunOptions(String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig)
        {
            TestRun.TestRunOptions testRunOptions = new TestRun.TestRunOptions();
            testRunOptions.environment = suiteTestRun.getEnvironmentName();
            testRunOptions.testArguments = suiteTestRun.getTemptoRunArguments(environmentConfig);
            testRunOptions.testJar = pathResolver.resolvePlaceholders(suiteRunOptions.testJar);
            testRunOptions.reportsDir = format("presto-product-tests/target/%s/%s/%s", suiteName, environmentConfig.getConfigName(), suiteTestRun.getEnvironmentName());
            return testRunOptions;
        }
    }

    private static class TestRunResult
    {
        private final SuiteTestRun suiteRun;
        private final EnvironmentConfig environmentConfig;
        private final Duration duration;
        private final Optional<Throwable> throwable;

        public TestRunResult(SuiteTestRun suiteRun, EnvironmentConfig environmentConfig, Duration duration, Optional<Throwable> throwable)
        {
            this.suiteRun = requireNonNull(suiteRun, "suiteRun is null");
            this.environmentConfig = requireNonNull(environmentConfig, "suiteConfig is null");
            this.duration = requireNonNull(duration, "duration is null");
            this.throwable = requireNonNull(throwable, "throwable is null");
        }

        public SuiteTestRun getSuiteRun()
        {
            return this.suiteRun;
        }

        public EnvironmentConfig getSuiteConfig()
        {
            return this.environmentConfig;
        }

        public Duration getDuration()
        {
            return this.duration;
        }

        public boolean isSuccessful()
        {
            return this.throwable.isEmpty();
        }

        public boolean hasFailed()
        {
            return this.throwable.isPresent();
        }

        public Optional<Throwable> getThrowable()
        {
            return this.throwable;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("suiteRun", suiteRun)
                    .add("suiteConfig", environmentConfig)
                    .add("duration", duration)
                    .add("throwable", throwable)
                    .toString();
        }
    }
}
