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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;

@Command(name = "modulesToConnectors", mixinStandardHelpOptions = true,
        description = "Maps Trino plugin modules to connectors they provide and filters them using an impacted modules list.")
public class PluginReader
        implements Callable<Integer>
{
    private static final Logger log = Logger.get(PluginReader.class);
    public static final String CONNECTOR = "connector:";
    public static final String BLOCK_ENCODING = "blockEncoding:";
    public static final String PARAMETRIC_TYPE = "parametricType:";
    public static final String FUNCTION = "function:";
    public static final String SYSTEM_ACCESS_CONTROL = "systemAccessControl:";
    public static final String GROUP_PROVIDER = "groupProvider:";
    public static final String PASSWORD_AUTHENTICATOR = "passwordAuthenticator:";
    public static final String HEADER_AUTHENTICATOR = "headerAuthenticator:";
    public static final String CERTIFICATE_AUTHENTICATOR = "certificateAuthenticator:";
    public static final String EVENT_LISTENER = "eventListener:";
    public static final String RESOURCE_GROUP_CONFIGURATION_MANAGER = "resourceGroupConfigurationManager:";
    public static final String SESSION_PROPERTY_CONFIGURATION_MANAGER = "sessionPropertyConfigurationManager:";
    public static final String EXCHANGE_MANAGER = "exchangeManager:";

    @Option(names = {"-i", "--impacted-modules"}, description = "Impacted modules file generated by the gitflow-incremental-builder (GIB) Maven plugin")
    private Optional<File> impactedModulesFile;

    @Option(names = {"-p", "--plugin-dir"}, description = "Trino plugin directory")
    private File pluginDir = new File("plugin");

    @Option(names = {"-r", "--root-pom"}, description = "Trino root module pom.xml")
    private File rootPom = new File("pom.xml");

    public static void main(String... args)
    {
        int exitCode = new CommandLine(new PluginReader()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call()
    {
        Optional<List<String>> impactedModules = Optional.empty();
        if (impactedModulesFile.isPresent()) {
            impactedModules = readImpactedModules(impactedModulesFile.get());
            if (impactedModules.isEmpty()) {
                return 1;
            }
        }
        Map<String, String> modulesToPlugins = mapModulesToPlugins(rootPom);
        Stream<Map.Entry<String, String>> modulesStream = modulesToPlugins.entrySet().stream();
        if (impactedModules.isPresent()) {
            List<String> nonPluginModules = impactedModules.get().stream()
                    .filter(not(modulesToPlugins::containsKey))
                    .toList();
            if (!nonPluginModules.isEmpty()) {
                log.warn("Impacted modules list includes non-plugin modules, ignoring it: %s", nonPluginModules);
            }
            else {
                List<String> finalImpactedModules = impactedModules.get();
                modulesStream = modulesStream.filter(entry -> finalImpactedModules.contains(entry.getKey()));
            }
        }

        Map<String, Plugin> plugins = loadPlugins(pluginDir).stream()
                .collect(toMap(plugin -> plugin.getClass().getName(), identity()));
        modulesStream.forEach(entry -> {
            if (!plugins.containsKey(entry.getValue())) {
                log.warn("Plugin without any connectors: %s", entry.getValue());
                return;
            }
            printPluginFeatures(plugins.get(entry.getValue()));
        });
        return 0;
    }

    private static void printPluginFeatures(Plugin plugin)
    {
        plugin.getConnectorFactories().forEach(factory -> System.out.println(CONNECTOR + factory.getName()));
        plugin.getBlockEncodings().forEach(encoding -> System.out.println(BLOCK_ENCODING + encoding.getName()));
        plugin.getTypes().forEach(type -> System.out.println(type.getTypeId()));
        plugin.getParametricTypes().forEach(type -> System.out.println(PARAMETRIC_TYPE + type.getName()));
        plugin.getFunctions().forEach(functionClass -> extractFunctions(functionClass)
                .getFunctions()
                .forEach(function -> System.out.println(FUNCTION + function.getSignature())));
        plugin.getSystemAccessControlFactories().forEach(factory -> System.out.println(SYSTEM_ACCESS_CONTROL + factory.getName()));
        plugin.getGroupProviderFactories().forEach(factory -> System.out.println(GROUP_PROVIDER + factory.getName()));
        plugin.getPasswordAuthenticatorFactories().forEach(factory -> System.out.println(PASSWORD_AUTHENTICATOR + factory.getName()));
        plugin.getHeaderAuthenticatorFactories().forEach(factory -> System.out.println(HEADER_AUTHENTICATOR + factory.getName()));
        plugin.getCertificateAuthenticatorFactories().forEach(factory -> System.out.println(CERTIFICATE_AUTHENTICATOR + factory.getName()));
        plugin.getEventListenerFactories().forEach(factory -> System.out.println(EVENT_LISTENER + factory.getName()));
        plugin.getResourceGroupConfigurationManagerFactories().forEach(factory -> System.out.println(RESOURCE_GROUP_CONFIGURATION_MANAGER + factory.getName()));
        plugin.getSessionPropertyConfigurationManagerFactories().forEach(factory -> System.out.println(SESSION_PROPERTY_CONFIGURATION_MANAGER + factory.getName()));
        plugin.getExchangeManagerFactories().forEach(factory -> System.out.println(EXCHANGE_MANAGER + factory.getName()));
    }

    private static Map<String, String> mapModulesToPlugins(File rootPom)
    {
        List<String> modules = readTrinoPlugins(rootPom);
        return modules.stream()
                .collect(toMap(identity(), module -> readPluginClassName(rootPom, module)));
    }

    private static List<String> readTrinoPlugins(File rootPom)
    {
        try (FileReader fileReader = new FileReader(rootPom, UTF_8)) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(fileReader);
            return model.getModules().stream()
                    .filter(module -> isTrinoPlugin(requireNonNullElse(rootPom.getParent(), ".") + "/" + module))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read file %s", rootPom), e);
        }
        catch (XmlPullParserException e) {
            throw new RuntimeException(format("Couldn't parse file %s", rootPom), e);
        }
    }

    private static boolean isTrinoPlugin(String module)
    {
        String modulePom = module + "/pom.xml";
        try (FileReader fileReader = new FileReader(modulePom, UTF_8)) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(fileReader);
            return model.getPackaging().equals("trino-plugin");
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read file %s", modulePom), e);
        }
        catch (XmlPullParserException e) {
            throw new RuntimeException(format("Couldn't parse file %s", modulePom), e);
        }
    }

    private static String readPluginClassName(File rootPom, String module)
    {
        Path target = Path.of(requireNonNullElse(rootPom.getParent(), "."), module, "target");
        BiPredicate<Path, BasicFileAttributes> matcher = (path, attributes) -> path.toFile().getName().matches(".*-services\\.jar");
        try (Stream<Path> files = java.nio.file.Files.find(target, 1, matcher)) {
            return files.findFirst()
                    .map(jarFile -> readPluginClassName(jarFile.toFile()))
                    .orElseThrow(() -> new MissingResourceException(
                            format("Couldn't find plugin name in services jar for module %s", module),
                            Plugin.class.getName(),
                            module));
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read services jar for module %s", module), e);
        }
    }

    private static String readPluginClassName(File serviceJar)
    {
        try {
            ZipFile zipFile = new ZipFile(serviceJar);
            return zipFile.stream()
                    .filter(entry -> !entry.isDirectory() && entry.getName().equals("META-INF/services/io.trino.spi.Plugin"))
                    .findFirst()
                    .map(entry -> {
                        try (BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(entry))) {
                            return new String(ByteStreams.toByteArray(bis), UTF_8).trim();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(format("Couldn't read plugin's service descriptor in %s", serviceJar), e);
                        }
                    })
                    .orElseThrow(() -> new MissingResourceException(
                            format("Couldn't find 'META-INF/services/io.trino.spi.Plugin' file in the service JAR %s", serviceJar.getPath()),
                            Plugin.class.getName(),
                            serviceJar.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't process service JAR %s", serviceJar), e);
        }
    }

    private static Optional<List<String>> readImpactedModules(File gibImpactedModules)
    {
        try {
            return Optional.of(Files.readAllLines(gibImpactedModules.toPath()));
        }
        catch (IOException e) {
            log.warn(e, "Couldn't read file %s", gibImpactedModules);
            return Optional.empty();
        }
    }

    private static List<Plugin> loadPlugins(File path)
    {
        ServerPluginsProviderConfig config = new ServerPluginsProviderConfig();
        config.setInstalledPluginsDir(path);
        ServerPluginsProvider pluginsProvider = new ServerPluginsProvider(config, directExecutor());
        ImmutableList.Builder<Plugin> plugins = ImmutableList.builder();
        pluginsProvider.loadPlugins((plugin, createClassLoader) -> loadPlugin(createClassLoader, plugins), PluginManager::createClassLoader);
        return plugins.build();
    }

    private static void loadPlugin(Supplier<PluginClassLoader> createClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        PluginClassLoader pluginClassLoader = createClassLoader.get();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadServicePlugin(pluginClassLoader, plugins);
        }
    }

    private static void loadServicePlugin(PluginClassLoader pluginClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> loadedPlugins = ImmutableList.copyOf(serviceLoader);
        checkState(!loadedPlugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));
        plugins.addAll(loadedPlugins);
    }
}
