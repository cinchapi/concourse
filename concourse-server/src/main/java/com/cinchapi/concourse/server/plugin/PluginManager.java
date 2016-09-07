/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.HeapBuffer;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.ZipException;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.JavaApp;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.MorePaths;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.ZipFiles;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.CharStreams;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
 * are placed on a {@link PluginInfoColumn#FROM_PLUGIN_RESPONSES message queue}
 * that is read by the local worker threads.</li>
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
 * <em>It is worth noting that plugins can indirectly communicate with one another
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
     * The directory of plugins that are managed by this {@link PluginManager}.
     */
    private final String home;

    // TODO make the plugin launcher watch the directory for changes/additions
    // and when new plugins are added, it should launch them

    /**
     * A table that contains metadata about the plugins managed herewithin.
     * (endpoint_class (id) | plugin | shared_memory_path | status |
     * app_instance)
     */
    private final Table<String, PluginInfoColumn, Object> router = HashBasedTable
            .create();

    /**
     * All the {@link SharedMemory streams} for which real time data updates are
     * sent.
     */
    private final Set<SharedMemory> streams = Collections.newSetFromMap(Maps
            .<SharedMemory, Boolean> newConcurrentMap());

    /**
     * The template to use when creating {@link JavaApp external java processes}
     * to run the plugin code.
     */
    private String template;

    /**
     * Construct a new instance.
     * 
     * @param directory
     */
    public PluginManager(String directory) {
        this.home = Paths.get(directory).toAbsolutePath().toString();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                stop();
            }

        }));
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
            String manifest = ZipFiles.getEntryContent(bundle, basename
                    + File.separator + MANIFEST_FILE);
            JsonObject json = (JsonObject) new JsonParser().parse(manifest);
            name = json.get("bundleName").getAsString();
            ZipFiles.unzip(bundle, home);
            File src = new File(home + File.separator + basename);
            File dest = new File(home + File.separator + name);
            src.renameTo(dest);
            Logger.info("Installed the plugins from {} at {}", bundle,
                    dest.getAbsolutePath());
            activate(name);
        }
        catch (Exception e) {
            Logger.error("Plugin bundle installation error:", e);
            Throwable cause = null;
            if((cause = e.getCause()) != null && cause instanceof ZipException) {
                throw new RuntimeException(bundle
                        + " is not a valid plugin bundle: "
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
     * @param clazz the {@link Plugin} endpoint class
     * @param method the name of the method to invoke
     * @param args a list of arguments to pass to the method
     * @param creds the {@link AccessToken} submitted to ConcourseServer via the
     *            invokePlugin method
     * @param transaction the {@link TransactionToken} submitted to
     *            ConcourseServer via the invokePlugin method
     * @param environment the environment submitted to ConcourseServer via the
     *            invokePlugin method
     * @return the response from the plugin
     */
    public ComplexTObject invoke(String clazz, String method,
            List<ComplexTObject> args, final AccessToken creds,
            TransactionToken transaction, String environment) {
        SharedMemory fromServer = (SharedMemory) router.get(clazz,
                PluginInfoColumn.FROM_SERVER);
        if(fromServer == null) {
            throw new PluginException(Strings.format(
                    "No plugin with id {} exists", clazz));
        }
        RemoteMethodRequest request = new RemoteMethodRequest(method, creds,
                transaction, environment, args);
        Buffer buffer = request.serialize();
        fromServer.write(ByteBuffer.wrap(((HeapBuffer) buffer).array()));
        ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) router
                .get(clazz, PluginInfoColumn.FROM_PLUGIN_RESPONSES);
        RemoteMethodResponse response = ConcurrentMaps.waitAndRemove(
                fromPluginResponses, creds);
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
     */
    public void start() {
        this.template = FileSystem.read(Resources
                .getAbsolutePath("/META-INF/ConcoursePlugin.tpl"));
        for (String plugin : FileSystem.getSubDirs(home)) {
            activate(plugin);
        }
    }

    /**
     * Stop the plugin manager and shutdown any managed plugins that are
     * running.
     */
    public void stop() {
        for (String id : router.rowKeySet()) {
            JavaApp app = (JavaApp) router.get(id,
                    PluginInfoColumn.APP_INSTANCE);
            app.destroy();
        }
        router.clear();
    }

    /**
     * Uninstall the plugin {@code bundle}
     * 
     * @param bundle the name of the plugin bundle
     */
    public void uninstallBundle(String bundle) {
        // TODO implement me
        /*
         * make sure all the plugins in the bundle are stopped
         * delete the bundle directory
         */
        FileSystem.deleteDirectory(home + File.separator + bundle);
    }

    /**
     * Get all the {@link Plugin plugins} in the {@code bundle} and
     * {@link #launch(String, Path, Class, List) launch} them each in a separate
     * JVM.
     * 
     * @param bundle the path to a bundle directory, which is a sub-directory of
     *            the {@link #home} directory.
     */
    protected void activate(String bundle) {
        activate(bundle, false);
    }

    /**
     * Get all the {@link Plugin plugins} in the {@code bundle} and
     * {@link #launch(String, Path, Class, List) launch} them each in a separate
     * JVM.
     * 
     * @param bundle the path to a bundle directory, which is a sub-directory of
     *            the {@link #home} directory.
     * @param runAfterInstallHook a flag that indicates whether the
     *            {@link Plugin#afterInstall()} hook should be run
     */
    protected void activate(String bundle, boolean runAfterInstallHook) {
        try {
            String lib = home + File.separator + bundle + File.separator
                    + "lib" + File.separator;
            Path prefs = Paths.get(home, bundle,
                    PluginConfiguration.PLUGIN_PREFS_FILENAME);
            Iterator<Path> content = Files.newDirectoryStream(Paths.get(lib))
                    .iterator();

            // Go through all the jars in the plugin's lib directory and compile
            // the appropriate classpath while identifying jars that might
            // contain plugin endpoints.
            List<URL> urls = Lists.newArrayList();
            List<String> classpath = Lists.newArrayList();
            while (content.hasNext()) {
                String filename = content.next().getFileName().toString();
                URL url = new File(lib + filename).toURI().toURL();
                if(!SYSTEM_JARS.contains(filename)) {
                    // NOTE: by checking for exact name matches, we will
                    // accidentally include system jars that contain different
                    // versions.
                    urls.add(url);
                }
                classpath.add(url.getFile());
            }

            // Create a ClassLoader that only contains jars with possible plugin
            // endpoints and search for any applicable classes.
            URLClassLoader loader = new URLClassLoader(
                    urls.toArray(new URL[0]), null);
            Class parent = loader.loadClass(Plugin.class.getName());
            Class realTimeParent = loader.loadClass(RealTimePlugin.class
                    .getName());
            Reflections reflection = new Reflections(new ConfigurationBuilder()
                    .addClassLoader(loader).addUrls(
                            ClasspathHelper.forClassLoader(loader)));
            Set<Class<?>> subTypes = reflection.getSubTypesOf(parent);
            Iterable<Class<?>> plugins = subTypes.stream().filter(
                    (clz) -> !clz.isInterface()
                            && !Modifier.isAbstract(clz.getModifiers()))::iterator;
            for (final Class<?> plugin : plugins) {
                if(runAfterInstallHook) {
                    Object instance = Reflection.newInstance(plugin);
                    Reflection.call(instance, "afterInstall");
                }
                launch(bundle, prefs, plugin, classpath);
                startEventLoop(plugin.getName());
                if(realTimeParent.isAssignableFrom(plugin)) {
                    startRealTimeStream(plugin.getName());
                }
            }

        }
        catch (IOException | ClassNotFoundException e) {
            Logger.error(
                    "An error occurred while trying to activate the plugin bundle '{}'",
                    bundle, e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Launch the {@code plugin} from {@code dist} within a separate JVM
     * configured with the specified {@code classpath} and the values from the
     * {@code prefs} file.
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
        String fromServer = FileSystem.tempFile();
        String fromPlugin = FileSystem.tempFile();
        String source = template
                .replace("INSERT_IMPORT_STATEMENT", launchClass)
                .replace("INSERT_FROM_SERVER", fromServer)
                .replace("INSERT_FROM_PLUGIN", fromPlugin)
                .replace("INSERT_CLASS_NAME", launchClassShort);

        // Create an external JavaApp in which the Plugin will run. Get the
        // plugin config to size the JVM properly.
        PluginConfiguration config = Reflection.newInstance(
                StandardPluginConfiguration.class, prefs);
        long heapSize = config.getHeapSize() / BYTES_PER_MB;
        String pluginHome = home + File.separator + bundle;
        String[] options = new String[] { "-Xms" + heapSize + "M",
                "-Xmx" + heapSize + "M",
                "-D" + Plugin.PLUGIN_HOME_JVM_PROPERTY + "=" + pluginHome };
        JavaApp app = new JavaApp(StringUtils.join(classpath,
                JavaApp.CLASSPATH_SEPARATOR), source, options);
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
                // TODO: it would be nice to just restart the same JavaApp
                // instance (e.g. app.restart();)
                launch(bundle, prefs, plugin, classpath);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });

        // Store metadata about the Plugin
        String id = launchClass;
        router.put(id, PluginInfoColumn.PLUGIN_BUNDLE, bundle);
        router.put(id, PluginInfoColumn.FROM_SERVER, new SharedMemory(
                fromServer));
        router.put(id, PluginInfoColumn.FROM_PLUGIN, new SharedMemory(
                fromPlugin));
        router.put(id, PluginInfoColumn.STATUS, PluginStatus.ACTIVE);
        router.put(id, PluginInfoColumn.APP_INSTANCE, app);
        router.put(id, PluginInfoColumn.FROM_PLUGIN_RESPONSES,
                Maps.<AccessToken, RemoteMethodResponse> newConcurrentMap());
    }

    /**
     * Start a {@link Thread} that serves as an event loop; processing both
     * requests and responses {@code #fromPlugin}.
     * <p>
     * Requests are forked to a {@link RemoteInvocationThread} for processing.
     * </p>
     * <p>
     * Responses are placed on the appropriate
     * {@link PluginInfoColumn#FROM_PLUGIN_RESPONSES queue} and listeners are
     * notified.
     * </p>
     * 
     * @param id the plugin id
     * @return the event loop thread
     */
    private Thread startEventLoop(String id) {
        final SharedMemory incoming = (SharedMemory) router.get(id,
                PluginInfoColumn.FROM_PLUGIN);
        final SharedMemory outgoing = (SharedMemory) router.get(id,
                PluginInfoColumn.FROM_SERVER);
        final ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) router
                .get(id, PluginInfoColumn.FROM_PLUGIN_RESPONSES);
        Thread loop = new Thread(new Runnable() {

            @Override
            public void run() {
                ByteBuffer data;
                while ((data = incoming.read()) != null) {
                    Buffer buffer = HeapBuffer.wrap(data.array());
                    RemoteMessage message = RemoteMessage.fromBuffer(buffer);
                    if(message.type() == RemoteMessage.Type.REQUEST) {
                        RemoteMethodRequest request = (RemoteMethodRequest) message;
                        Logger.debug("Received REQUEST from Plugin {}: {}", id,
                                request);
                        Thread worker = new RemoteInvocationThread(request,
                                outgoing, this, true, fromPluginResponses);
                        worker.start();
                    }
                    else if(message.type() == RemoteMessage.Type.RESPONSE) {
                        RemoteMethodResponse response = (RemoteMethodResponse) message;
                        Logger.debug("Received RESPONSE from Plugin {}: {}",
                                id, response);
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

        });
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
    private void startRealTimeStream(String id) {
        String streamFile = FileSystem.tempFile();
        SharedMemory stream = new SharedMemory(streamFile);
        RemoteAttributeExchange attribute = new RemoteAttributeExchange(
                "stream", streamFile);
        Buffer buffer = attribute.serialize();
        SharedMemory fromServer = (SharedMemory) router.get(id,
                PluginInfoColumn.FROM_SERVER);
        fromServer.write(ByteBuffer.wrap(((HeapBuffer) buffer).array()));
        streams.add(stream);
    }

    /**
     * The columns that are included in the {@link #router} table.
     * 
     * @author Jeff Nelson
     */
    private enum PluginInfoColumn {
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
         * loop},
         * which listen for messages on the associated {@code fromPlugin}
         * stream, encounters
         * a {@link Instruction#RESPONSE response} to an
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
        STATUS
    }

    /**
     * An enum to capture various statuses that plugins can have.
     * 
     * @author Jeff Nelson
     */
    private enum PluginStatus {
        ACTIVE;
    }

}
