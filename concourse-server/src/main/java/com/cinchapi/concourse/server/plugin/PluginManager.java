/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.plugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipException;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.JavaApp;
import com.cinchapi.concourse.server.plugin.data.WriteEvent;
import com.cinchapi.concourse.server.plugin.hook.AfterInstallHook;
import com.cinchapi.concourse.server.plugin.io.PluginSerializer;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.server.plugin.util.Versions;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.MorePaths;
import com.cinchapi.concourse.util.Queues;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.ZipFiles;
import com.github.zafarkhaja.semver.Version;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.BaseEncoding;
import com.google.common.io.CharStreams;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static com.cinchapi.concourse.server.GlobalState.BINARY_QUEUE;

/**
 * <p>
 * A {@link PluginManager} is responsible for handling all things (i.e.
 * starting, stopping, etc) related to plugins.
 * </p>
 * <h1>Working with Plugins</h1>
 * <p>
 * Any class that extends {@link Plugin} is considered a plugin. Plugins run in
 * a separate JVM, but are completely and transparently managed by this
 * {@link PluginManager class} via the utilities provided in the {@link JavaApp}
 * class.
 * </p>
 * <h2>Bundles</h2>
 * <p>
 * One or more plugins are packaged together in {@code bundles}. A bundle is
 * essentially a zip archive that contains the Plugin class(es) and all
 * necessary dependencies. All the plugins in a bundle are
 * {@link #installBundle(String) installed} and {@link #uninstallBundle(String)
 * uninstalled} together.
 * </p>
 * <p>
 * Usually, a bundle will only contain a single plugin; however, it is possible
 * to bundle multiple plugins if they all have same dependencies. When a bundle
 * contains multiple plugins, each of the plugins is still launch and managed in
 * a separate JVM
 * </p>
 * <h2>Plugin Lifecycle</h2>
 * <p>
 * When the PluginManager starts, it goes through all the bundles and
 * {@link #activate(String) activates} each Plugin. Part of the activation
 * routine is {@link #launch(String, Path, Class, List) launching} the plugin in
 * the external JVM.
 * </p>
 * <h2>Plugin Communication</h2>
 * <p>
 * Plugins communicate with Concourse Server via {@link SharedMemory} streams
 * setup by the {@link PluginManager}. Each plugin has two streams:
 * <ol>
 * <li>A {@code fromServer} stream that serves as a communication channel for
 * messages that come from Concourse Server and are read by the Plugin.
 * Internally, the plugin sets up an event loop that reads messages on the
 * {@code fromServer} stream and dispatches {@link Instruction#REQUEST requests}
 * to asynchronous worker threads while {@link Instruction#RESPONSE responses}
 * are placed on a message queue that is read by the local worker threads.</li>
 * <li>A {@code fromPlugin} stream that serves as a communication channel for
 * messages that come from Plugin and are read by the {@link PluginManager} on
 * behalf of Concourse Server. Internally, the PluginManager sets up an
 * {@link #startEventLoop(String) event loop} that reads messages on the
 * {@code fromPlugn} stream and dispatches {@link Instruction#REQUEST requests}
 * to asynchronous worker threads while {@link Instruction#RESPONSE responses}
 * are placed on a {@link RegistryData#FROM_PLUGIN_RESPONSES message queue} that
 * is read by the local worker threads.</li>
 * </ol>
 * </p>
 * <p>
 * Plugins are only allowed to communicate with Concourse Server (e.g. they
 * cannot communicate directly with other plugins).
 * </p>
 * <h1>Invoking Plugin Methods</h1>
 * <p>
 * Arbitrary plugin methods can be invoked using the
 * {@link #invoke(String, String, List, AccessToken, TransactionToken, String)}
 * method. The {@link PluginManager} passes these requests to the appropriate
 * plugin JVM via the {@code fromServer} {@link SharedMemory stream} that was
 * setup when the plugin launched.
 * </p>
 * <h1>Invoking Server Methods</h1>
 * <p>
 * Plugins are allowed to invoke server methods by placing the appropriate
 * request on the {@code fromPlugin} stream. When that happens, the
 * PluginManager will send responses back to the plugin on the
 * {@code fromServer} channel.
 * </p>
 * <p>
 * <em>It is worth noting that plugins can indirectly communicate with one
 * another
 * by sending a request to the server to invoke another plugin method.</em>
 * </p>
 * <h1>Real Time Plugins</h1>
 * <p>
 * Any plugin that extends {@link RealTimePlugin} will initially receive a
 * {@link SharedMemory} segment for real time communication data streams. This
 * is a one-way stream. Plugins are responsible for decide when and how to
 * respond to data that is streamed over.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class PluginManager {

    /**
     * Return the correct temporary directory that should be used to store
     * temporary files (i.e. shared memory segments, etc) for the
     * {@code plugin}.
     * 
     * @param plugin the fully qualified name of the plugin
     * @return the temporary directory that should be used for the
     *         {@code plugin}
     */
    private static String getPluginTempDirectory(String plugin) {
        Path baseTempDir = Paths.get(FileSystem.tempFile()).toFile()
                .getParentFile().toPath();
        Path sessionTempDir = baseTempDir.resolve(SESSID);
        Path pluginTempDir = sessionTempDir.resolve(plugin);
        return pluginTempDir.toString();
    }

    /**
     * The number of bytes in a MiB.
     */
    private static final long BYTES_PER_MB = 1048576;

    /**
     * The name of the manifest file that should be included with every plugin.
     */
    private static String MANIFEST_FILE = "manifest.json";

    /**
     * A collection of jar files that exist on the server's native classpath. We
     * keep track of these so that we don't unnecessarily search them for
     * plugins.
     */
    private static Set<String> SYSTEM_JARS;

    static {
        SYSTEM_JARS = Sets.newHashSet();
        ClassLoader cl = PluginManager.class.getClassLoader();
        if(cl instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader) cl).getURLs()) {
                String filename = MorePaths.get(url.getFile()).getFileName()
                        .toString();
                if(filename.endsWith(".jar")) {
                    SYSTEM_JARS.add(filename);
                }
            }
        }
        Reflections.log = null;
    }

    /**
     * Map of aliases names and its respective plugin id.
     */
    private Map<String, String> aliases = Maps.newHashMap();

    /**
     * List of aliases that are restricted to use and are set as ambiguous names
     * to use.
     */
    private Set<String> ambiguous = Sets.newHashSet();

    /**
     * A collection of each of the install bundles, mapped to the associated
     * {@link Version}.
     */
    private Map<String, Version> bundles = Maps.newHashMap();

    /**
     * {@link ExecutorService} to stream {@link Packet packets} asynchronously.
     */
    private final ExecutorService executor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * The directory of plugins that are managed by this {@link PluginManager}.
     */
    private final String home;

    /**
     * A table that contains metadata about the plugins managed herewithin:
     * <ul>
     * <li>class (primary key)</li>
     * <li>bundle</li>
     * <li>fromPlugin</li>
     * <li>fromPluginResponses</li>
     * <li>fromServer</li>
     * <li>appInstance</li>
     * <li>status</li>
     * </ul>
     */
    private final Table<String, RegistryData, Object> registry = HashBasedTable
            .create();

    // TODO make the plugin launcher watch the directory for changes/additions
    // and when new plugins are added, it should launch them

    /**
     * A flag that indicates if the manager is running or not.
     */
    private boolean running = false;

    /**
     * Responsible for taking arbitrary objects and turning them into binary so
     * they can be sent across the wire.
     */
    private final PluginSerializer serializer = new PluginSerializer();

    /**
     * The host server in which this {@link PluginManager} runs.
     */
    private final ConcourseServer server;

    /**
     * The thread that loops through the {@link GlobalState#BINARY_QUEUE} to get
     * writes that must be streamed to real time plugins
     */
    private final Thread streamLoop;

    /**
     * All the {@link SharedMemory streams} for which real time data updates are
     * sent.
     */
    private final Set<SharedMemory> streams = Collections
            .newSetFromMap(Maps.<SharedMemory, Boolean> newConcurrentMap());

    /**
     * The template to use when creating {@link JavaApp external java processes}
     * to run the plugin code.
     */
    private String pluginLaunchClassTemplate;

    /**
     * The session id for the {@link PluginManager}. This is used for grouping
     * shared memory files.
     */
    private final static String SESSID = Long.toString(Time.now());

    /**
     * Construct a new instance.
     *
     * @param directory
     */
    public PluginManager(ConcourseServer server, String directory) {
        this.server = server;
        this.home = Paths.get(directory).toAbsolutePath().toString();
        this.streamLoop = new Thread(() -> {
            outer: while (true && !Thread.interrupted()) {
                // The stream loop continuously checks the BINARY_QUEUE
                // for new writes to stream to all the RealTime plugins.
                List<WriteEvent> events = Lists.newArrayList();
                try {
                    Queues.blockingDrain(BINARY_QUEUE, events);
                }
                catch (InterruptedException e) {
                    // Assume that the #stop routine is interrupting because it
                    // wants this thread to terminate.
                    break;
                }
                if(streams.size() > 0) {
                    final Packet packet = new Packet(events);
                    Logger.debug(
                            "Streaming packet to real-time " + "plugins: {}",
                            packet);
                    final ByteBuffer data = serializer.serialize(packet);
                    List<Future<SharedMemory>> awaiting = Lists.newArrayList();
                    for (SharedMemory stream : streams) {
                        awaiting.add(executor.submit(() -> stream.write(data)));
                    }
                    for (Future<SharedMemory> status : awaiting) {
                        // Ensure that the Packet was written to all the streams
                        // before looping again so that Packets are not sent out
                        // of order.
                        try {
                            status.get();
                        }
                        catch (InterruptedException e) {
                            // Assume that the #stop routine is interrupting
                            // because it wants this thread to terminate.
                            break outer;
                        }
                        catch (ExecutionException e) {
                            Logger.error("Exeception occurred while streaming "
                                    + "data to a plugin: ", e);
                        }
                    }
                }
                else {
                    Logger.debug(
                            "No real-time plugins are installed "
                                    + "but the following events have been "
                                    + "drained from the BINARY_QUEUE: {}",
                            events);
                }
            }
        });
        streamLoop.setName("plugin-manager-stream-loop");
        streamLoop.setDaemon(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop()));
    }

    /**
     * Install the plugin bundle located within a zip file to the {@link #home}
     * directory.
     *
     * @param bundle the path to the plugin bundle
     */
    public void installBundle(String bundle) {
        String basename = com.google.common.io.Files
                .getNameWithoutExtension(bundle);
        String name = null;
        try {
            String manifest = ZipFiles.getEntryContent(bundle,
                    basename + File.separator + MANIFEST_FILE);
            JsonObject json = (JsonObject) new JsonParser().parse(manifest);
            name = json.get("bundleName").getAsString();
            File dest = new File(home + File.separator + name);
            if(!dest.exists()) {
                File src = new File(home + File.separator + basename);
                ZipFiles.unzip(bundle, home);
                src.renameTo(dest);
                Logger.info("Installed the plugins from {} at {}", bundle,
                        dest.getAbsolutePath());
                activate(name, ActivationType.INSTALL);
            }
            else {
                String message = name + " is already installed. "
                        + "Please use the upgrade option to install a newer "
                        + "version.";
                name = null;
                throw new IllegalStateException(message);
            }
        }
        catch (Exception e) {
            Logger.error("Plugin bundle installation error:", e);
            Throwable cause = null;
            if((cause = e.getCause()) != null
                    && cause instanceof ZipException) {
                throw new PluginInstallException(
                        bundle + " is not a valid plugin bundle: "
                                + cause.getMessage());
            }
            else {
                if(name != null) {
                    // Likely indicates that there was a problem with
                    // activation, so run uninstall path so things are not in a
                    // weird state
                    uninstallBundle(name);
                }
                throw e; // re-throw exception so CLI fails
            }
        }
    }

    /**
     * Invoke {@code method} that is defined in the plugin endpoint inside of
     * {@clazz}. The provided {@code creds}, {@code transaction} token and
     * {@code environment} are used to ensure proper alignment with the
     * corresponding client session on the server.
     *
     * @param plugin class or alias name of the {@link Plugin}
     * @param method the name of the method to invoke
     * @param args a list of arguments to pass to the method
     * @param creds the {@link AccessToken} submitted to ConcourseServer via the
     *            invokePlugin method
     * @param transaction the {@link TransactionToken} submitted to
     *            ConcourseServer via
     *            the invokePlugin method
     * @param environment the environment submitted to ConcourseServer via the
     *            invokePlugin method
     * @return the response from the plugin
     */
    public ComplexTObject invoke(String plugin, String method,
            List<ComplexTObject> args, final AccessToken creds,
            TransactionToken transaction, String environment) {
        String clazz = getIdByAlias(plugin);
        SharedMemory fromServer = (SharedMemory) registry.get(clazz,
                RegistryData.FROM_SERVER);
        if(fromServer == null) {
            String message = ambiguous.contains(plugin)
                    ? "Multiple plugins are "
                            + "configured to use the alias '{}' so it is not permitted. "
                            + "Please invoke the plugin using its full qualified name"
                    : "No plugin with id or alias {} exists";
            throw new PluginException(Strings.format(message, clazz));
        }
        RemoteMethodRequest request = new RemoteMethodRequest(method, creds,
                transaction, environment, args);
        ByteBuffer buffer = serializer.serialize(request);
        fromServer.write(buffer);
        ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) registry
                .get(clazz, RegistryData.FROM_PLUGIN_RESPONSES);
        RemoteMethodResponse response = ConcurrentMaps
                .waitAndRemove(fromPluginResponses, creds);
        if(!response.isError()) {
            return response.response;
        }
        else {
            throw Throwables.propagate(response.error);
        }
    }

    /**
     * Return the names of all the plugins available in the {@link #home}
     * directory.
     *
     * @return the available plugins
     */
    public Set<String> listBundles() {
        return FileSystem.getSubDirs(home);
    }

    /**
     * Start the plugin manager.
     * <p>
     * This also starts to stream {@link Packet} in separate thread
     * </p>
     */
    public void start() {
        if(!running) {
            running = true;
            streamLoop.start();
            pluginLaunchClassTemplate = FileSystem.read(
                    Resources.getAbsolutePath("/META-INF/ConcoursePlugin.tpl"));
            for (String bundle : FileSystem.getSubDirs(home)) {
                activate(bundle, ActivationType.START);
            }
        }
    }

    /**
     * Stop the plugin manager and shutdown any managed plugins that are
     * running.
     */
    public void stop() {
        streamLoop.interrupt();
        executor.shutdownNow();
        for (String id : registry.rowKeySet()) {
            JavaApp app = (JavaApp) registry.get(id, RegistryData.APP_INSTANCE);
            app.destroy();
        }
        registry.clear();
        running = false;
    }

    /**
     * Uninstall the plugin {@code bundle}
     *
     * @param bundle the name of the plugin bundle
     */
    public void uninstallBundle(String bundle) {
        // TODO implement me
        /*
         * make sure all the plugins in the bundle are stopped delete the bundle
         * directory. Will need to add a shutdown(plugin) method. And in
         * shutdown if there were no real time streams, then we should set
         * streamEnabled to false
         */
        FileSystem.deleteDirectory(home + File.separator + bundle);
    }

    /**
     * Activating a {@code bundle} means that all the plugins with the bundle
     * are loaded from disk and stored within the {@link #registry}. Depending
     * on the {@code type} some pre-launch hooks may be run. If all those hooks
     * are successful, each of the plugins in the bundle are
     * {@link #launch(String, Path, Class, List) launched}.
     *
     * @param bundle the name of the plugin bundle
     * @param type the {@link ActivationType type} of activation
     */
    protected void activate(String bundle, ActivationType type) {
        Logger.debug("Activating plugins from {}", bundle);
        Path home = Paths.get(this.home, bundle);
        Path lib = home.resolve("lib");
        Path prefs = home.resolve("conf")
                .resolve(PluginConfiguration.PLUGIN_PREFS_FILENAME);
        Path prefsDev = home.resolve("conf")
                .resolve(PluginConfiguration.PLUGIN_PREFS_DEV_FILENAME);
        if(Files.exists(prefsDev)) {
            prefs = prefsDev;
            prefsDev = null;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(lib)) {
            Iterator<Path> jars = stream.iterator();
            // Go through all the jars in the plugin's lib directory and compile
            // the appropriate classpath while identifying jars that might
            // contain plugin endpoints.
            List<URL> urls = Lists.newArrayList();
            List<String> classpath = Lists.newArrayList();
            while (jars.hasNext()) {
                String filename = jars.next().getFileName().toString();
                Path path = lib.resolve(filename);
                URL url = new File(path.toString()).toURI().toURL();
                if(!SYSTEM_JARS.contains(filename)
                        || type.mightRequireHooks()) {
                    // NOTE: by checking for exact name matches, we will
                    // accidentally include system jars that contain different
                    // versions.

                    // NOTE: if a hook must be run, we have to include all the
                    // jars (including system ones) so that the full context in
                    // which the hook was written is available.
                    urls.add(url);
                }
                classpath.add(url.getFile());
            }
            // Create a ClassLoader that only contains jars with possible plugin
            // endpoints and search for any applicable classes.
            URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[0]),
                    null);
            Class parent = loader.loadClass(Plugin.class.getName());
            Class realTimeParent = loader
                    .loadClass(RealTimePlugin.class.getName());
            Reflections reflection = new Reflections(
                    new ConfigurationBuilder().addClassLoader(loader)
                            .addUrls(ClasspathHelper.forClassLoader(loader)));
            Set<Class<?>> subTypes = reflection.getSubTypesOf(parent);
            Iterable<Class<?>> plugins = subTypes.stream()
                    .filter((clz) -> !clz.isInterface() && !Modifier
                            .isAbstract(clz.getModifiers()))::iterator;
            JsonObject manifest = loadBundleManifest(bundle);
            Version version = Versions.parseSemanticVersion(
                    manifest.get("bundleVersion").getAsString());
            for (final Class<?> plugin : plugins) {
                boolean launch = true;
                List<Throwable> errors = Lists.newArrayListWithCapacity(0);
                // Depending on the activation type, we may need to run some
                // hooks to determine if the plugins from the bundle can
                // actually be launched
                if(type.mightRequireHooks()) {
                    try {
                        Class contextClass = loader
                                .loadClass(PluginContext.class.getName());
                        Constructor contextConstructor = contextClass
                                .getDeclaredConstructor(Path.class,
                                        String.class, String.class);
                        contextConstructor.setAccessible(true);
                        String concourseVersion = Versions.parseSemanticVersion(
                                com.cinchapi.concourse.util.Version
                                        .getVersion(ConcourseServer.class)
                                        .toString())
                                .toString();
                        Object context = contextConstructor.newInstance(home,
                                version.toString(), concourseVersion);
                        Class iface;
                        switch (type) {
                        case INSTALL:
                        default:
                            iface = loader.loadClass(
                                    AfterInstallHook.class.getName());
                            break;
                        }
                        Set<Class<?>> potential = reflection
                                .getSubTypesOf(iface);
                        Iterable<Class<?>> hooks = potential.stream()
                                .filter((hook) -> !hook.isInterface()
                                        && !Modifier.isAbstract(
                                                hook.getModifiers()))::iterator;
                        for (Class<?> hook : hooks) {
                            Logger.info("Running hook '{}' for plugin '{}'",
                                    hook.getName(), plugin);
                            Object instance = Reflection.newInstance(hook);
                            Reflection.call(instance, "run", context);
                        }
                    }
                    catch (Exception e) {
                        Throwable error = Throwables.getRootCause(e);
                        Logger.error("Could not run {} hook for {}:", type,
                                plugin, error);
                        launch = false;
                        errors.add(error);
                    }
                }
                if(launch && errors.isEmpty()) {
                    launch(bundle, prefs, plugin, classpath);
                    startEventLoop(plugin.getName());
                    if(realTimeParent.isAssignableFrom(plugin)) {
                        startStreamToPlugin(plugin.getName());
                    }
                }
                else {
                    // Depending on the activation type, we respond differently
                    // to a pre-activation error. Plugins within a bundle are
                    // all or nothing, so if one of them fails the
                    // pre-activation checks then the entire bundle must suffer
                    // to consequences.
                    if(type == ActivationType.INSTALL) {
                        Logger.error("Errors occurred when trying to "
                                + "install {}: {}", bundle, errors);
                        throw new PluginInstallException("Could not install "
                                + bundle + " due to the following errors: "
                                + errors);
                    }
                    else {
                        Logger.error("An error occurred when trying to "
                                + "activate {}", bundle);
                        // TODO: call deactivate(bundle) whenever that method is
                        // ready
                    }
                    break;
                }
            }
            bundles.put(bundle, version);
        }
        catch (IOException | ClassNotFoundException e) {
            Logger.error(
                    "An error occurred while trying to activate the plugin bundle '{}'",
                    bundle, e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Returns the plugin registered for this alias. If unregistered, input
     * alias name is returned.
     *
     * @param alias
     * @return String plugin id or alias name.
     */
    private String getIdByAlias(String alias) {
        return aliases.getOrDefault(alias, alias);
    }

    /**
     * <p>
     * This method is called from {@link #activate(String, ActivationType)} once
     * any pre-launch checks have successfully completed.
     * </p>
     * <p>
     * Launch the {@code plugin} from {@code dist} within a separate JVM
     * configured with the specified {@code classpath} and the values from the
     * {@code prefs} file.
     * </p>
     *
     * @param bundle the bundle directory that contains the plugin libraries
     * @param prefs the {@link Path} to the config file
     * @param plugin the class to launch in a separate JVM
     * @param classpath the classpath for the separate JVM
     */
    private void launch(final String bundle, final Path prefs,
            final Class<?> plugin, final List<String> classpath) {
        // Write an arbitrary main class that'll construct the Plugin and run it
        String launchClass = plugin.getName();
        String launchClassShort = plugin.getSimpleName();
        String processName = "Concourse_" + launchClassShort;
        String tempDir = getPluginTempDirectory(launchClass);
        String fromServer = FileSystem.tempFile(tempDir, "FS-", ".shm");
        String fromPlugin = FileSystem.tempFile(tempDir, "FP-", ".shm");
        String source = pluginLaunchClassTemplate
                .replace("INSERT_PROCESS_NAME", processName)
                .replace("INSERT_IMPORT_STATEMENT", launchClass)
                .replace("INSERT_FROM_SERVER", fromServer)
                .replace("INSERT_FROM_PLUGIN", fromPlugin)
                .replace("INSERT_CLASS_NAME", launchClassShort);
        // Create an external JavaApp in which the Plugin will run. Get the
        // plugin config to size the JVM properly.
        PluginConfiguration config = Reflection
                .newInstance(StandardPluginConfiguration.class, prefs);
        Logger.info("Configuring plugin '{}' from bundle '{}' with "
                + "preferences located in {}", plugin, bundle, prefs);
        long heapSize = config.getHeapSize() / BYTES_PER_MB;
        for (String alias : config.getAliases()) {
            if(!aliases.containsKey(alias) && !ambiguous.contains(alias)) {
                aliases.put(alias, plugin.getName());
                Logger.info("Registering '{}' as an alias for {}", alias,
                        plugin);
            }
            else {
                aliases.remove(alias);
                ambiguous.add(alias);
                Logger.info("Alias '{}' can't be used because it is "
                        + "associated with multiple plugins", alias);
            }
        }
        String pluginHome = home + File.separator + bundle;
        String serviceToken = BaseEncoding.base32Hex()
                .encode(server.newServiceToken().bufferForData().array());
        ArrayList<String> options = new ArrayList<String>();
        if(config.getRemoteDebuggerEnabled()) {
            options.add("-Xdebug");
            options.add(
                    "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address="
                            + config.getRemoteDebuggerPort());
        }
        options.add("-Xms" + heapSize + "M");
        options.add("-Xmx" + heapSize + "M");
        options.add("-D" + Plugin.PLUGIN_HOME_JVM_PROPERTY + "=" + pluginHome);
        options.add("-D" + Plugin.PLUGIN_SERVICE_TOKEN_JVM_PROPERTY + "="
                + serviceToken);
        String cp = StringUtils.join(classpath, JavaApp.CLASSPATH_SEPARATOR);
        JavaApp app = new JavaApp(cp, source, options);
        app.run();
        if(app.isRunning()) {
            Logger.info("Starting plugin '{}' from bundle '{}'", launchClass,
                    bundle);
        }
        app.onPrematureShutdown((out, err) -> {
            try {
                List<String> outLines = CharStreams
                        .readLines(new InputStreamReader(out));
                List<String> errLines = CharStreams
                        .readLines(new InputStreamReader(err));
                Logger.warn("Plugin '{}' unexpectedly crashed. ", plugin);
                Logger.warn("Standard Output for {}: {}", plugin,
                        StringUtils.join(outLines, System.lineSeparator()));
                Logger.warn("Standard Error for {}: {}", plugin,
                        StringUtils.join(errLines, System.lineSeparator()));
                Logger.warn("Restarting {} now...", plugin);
                Iterator<Entry<String, String>> it = aliases.entrySet()
                        .iterator();
                while (it.hasNext()) {
                    Entry<String, String> entry = it.next();
                    if(entry.getValue().equals(plugin.getName())) {
                        it.remove();
                    }
                }
                // TODO: it would be nice to just restart the same JavaApp
                // instance (e.g. app.restart();)
                launch(bundle, prefs, plugin, classpath);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });

        // Ensure that the Plugin is ready to run before adding it to the
        // registry to avoid premature invocations
        Path readyCheck = com.cinchapi.common.io.Files
                .getHashedFilePath(serviceToken);
        try {
            while (!Files.deleteIfExists(readyCheck)) {
                Thread.sleep(1000);
                continue;
            }
            Logger.info("Plugin '{}' is ready", plugin);
        }
        catch (IOException | InterruptedException e) {}

        // Store metadata about the Plugin
        String id = launchClass;
        registry.put(id, RegistryData.PLUGIN_BUNDLE, bundle);
        registry.put(id, RegistryData.FROM_SERVER,
                new SharedMemory(fromServer));
        registry.put(id, RegistryData.FROM_PLUGIN,
                new SharedMemory(fromPlugin));
        registry.put(id, RegistryData.STATUS, PluginStatus.ACTIVE);
        registry.put(id, RegistryData.APP_INSTANCE, app);
        registry.put(id, RegistryData.FROM_PLUGIN_RESPONSES,
                Maps.<AccessToken, RemoteMethodResponse> newConcurrentMap());
        Logger.debug("Shared memory for server-based communication to '{} is "
                + "located at '{}", id, fromServer);
        Logger.debug("Shared memory for plugin-based communication from '{} is "
                + "located at '{}", id, fromPlugin);
    }

    /**
     * Load the {@code bundle}'s manifest from disk as a {@link JsonObject}.
     *
     * @param bundle the name of the bundle
     * @return a JsonObject with all the data in the bundle
     */
    private JsonObject loadBundleManifest(String bundle) {
        String manifest = FileSystem
                .read(Paths.get(home, bundle, MANIFEST_FILE).toString());
        return (JsonObject) new JsonParser().parse(manifest);
    }

    /**
     * Start a {@link Thread} that serves as an event loop; processing both
     * requests and responses {@code #fromPlugin}.
     * <p>
     * Requests are forked to a {@link RemoteInvocationThread} for processing.
     * </p>
     * <p>
     * Responses are placed on the appropriate
     * {@link RegistryData#FROM_PLUGIN_RESPONSES queue} and listeners are
     * notified.
     * </p>
     *
     * @param id the plugin id
     * @return the event loop thread
     */
    private Thread startEventLoop(String id) {
        final SharedMemory incoming = (SharedMemory) registry.get(id,
                RegistryData.FROM_PLUGIN);
        final SharedMemory outgoing = (SharedMemory) registry.get(id,
                RegistryData.FROM_SERVER);
        final ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) registry
                .get(id, RegistryData.FROM_PLUGIN_RESPONSES);
        Thread loop = new Thread(new Runnable() {

            @Override
            public void run() {
                ByteBuffer data;
                while ((data = incoming.read()) != null) {
                    RemoteMessage message = serializer.deserialize(data);
                    if(message.type() == RemoteMessage.Type.REQUEST) {
                        RemoteMethodRequest request = (RemoteMethodRequest) message;
                        Logger.debug("Received REQUEST from Plugin {}: {}", id,
                                request);
                        Thread worker = new RemoteInvocationThread(request,
                                outgoing, server, true, fromPluginResponses);
                        worker.setUncaughtExceptionHandler(
                                (thread, throwable) -> {
                                    Logger.error(
                                            "While processing request '{}' from '{}', the following "
                                                    + "non-recoverable error occurred:",
                                            request, id, throwable);
                                });
                        worker.start();
                    }
                    else if(message.type() == RemoteMessage.Type.RESPONSE) {
                        RemoteMethodResponse response = (RemoteMethodResponse) message;
                        Logger.debug("Received RESPONSE from Plugin {}: {}", id,
                                response);
                        ConcurrentMaps.putAndSignal(fromPluginResponses,
                                response.creds, response);
                    }
                    else if(message.type() == RemoteMessage.Type.STOP) {
                        break;
                    }
                    else {
                        // Ignore the message...
                        continue;
                    }
                }

            }

        }, "plugin-event-loop-" + id);
        loop.setDaemon(true);
        loop.start();
        return loop;
    }

    /**
     * Create a {@link SharedMemory} segment over which the PluginManager will
     * stream real-time {@link Packet packets} that contain writes.
     *
     * @param id the plugin id
     */
    private void startStreamToPlugin(String id) {
        String tempDir = getPluginTempDirectory(id);
        String streamFile = FileSystem.tempFile(tempDir, "RT-", ".shm");
        Logger.debug("Creating real-time stream for {} at {}", id, streamFile);
        SharedMemory stream = new SharedMemory(streamFile);
        Logger.debug("Shared memory for real-time stream of '{} is located at "
                + "'{}", id, streamFile);
        RemoteAttributeExchange attribute = new RemoteAttributeExchange(
                "stream", streamFile);
        SharedMemory fromServer = (SharedMemory) registry.get(id,
                RegistryData.FROM_SERVER);
        ByteBuffer buffer = serializer.serialize(attribute);
        fromServer.write(buffer);
        streams.add(stream);
    }

    /**
     * An enum that describes the various reason that the
     * {@link #activate(String, ActivationType)} method may be called.
     *
     * @author Jeff Nelson
     */
    private enum ActivationType {
        INSTALL, START;

        /**
         * Return {@code true} if this {@link ActivationType} may require one or
         * more hooks to be run.
         *
         * @return {@code true} if there may be a hook associated with this type
         */
        public boolean mightRequireHooks() {
            switch (this) {
            case INSTALL:
                return true;
            default:
                return false;
            }
        }
    }

    /**
     * An enum to capture various statuses that plugins can have.
     *
     * @author Jeff Nelson
     */
    private enum PluginStatus {
        ACTIVE;
    }

    /**
     * The columns that are included in the {@link #registry} table.
     *
     * @author Jeff Nelson
     */
    private enum RegistryData {
        /**
         * A reference to the {@link JavaApp} that manages the external JVM
         * process for the plugin.
         */
        APP_INSTANCE,

        /**
         * A reference to the {@link SharedMemory} stream that is used by the
         * {@link PluginManager} to listen to messages that come from the
         * plugin.
         */
        FROM_PLUGIN,

        /**
         * A reference to a {@link ConcurrentMap} that associates an
         * {@link AccessToken} to a {@link RemoteMethodResponse}. This
         * collection is created for each plugin upon being
         * {@link PluginManager#launch(String, Path, Class, List) launched}.
         * Whenever a plugin's {@link PluginManager#startEventLoop(String) event
         * loop}, which listen for messages on the associated {@code fromPlugin}
         * stream, encounters a {@link Instruction#RESPONSE response} to an
         * {@link PluginManager#invoke(String, String, List, AccessToken, TransactionToken, String)
         * invoke} request, the {@link RemoteMethodResponse response} is placed
         * in the map on which the dispatched {@link RemoteInvocationThread
         * worker} thread is
         * {@link ConcurrentMaps#waitAndRemove(ConcurrentMap, Object) waiting}.
         */
        FROM_PLUGIN_RESPONSES,

        /**
         * A reference to the {@link SharedMemory} stream that is used by the
         * {@link PluginManager} to send messages to the plugin. The plugin has
         * an event loop that listens on this stream.
         */
        FROM_SERVER,

        /**
         * The name of the bundle in which the plugin is contained. This is
         * useful for finding all the plugin that belong to a bundle and need to
         * be {@link PluginManager#uninstallBundle(String) uninstalled}.
         */
        PLUGIN_BUNDLE,

        /**
         * A flag that contains the {@link PluginStatus status} for the plugin.
         */
        STATUS,
    }

}
