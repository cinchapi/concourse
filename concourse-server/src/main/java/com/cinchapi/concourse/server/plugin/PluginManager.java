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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.JavaApp;
import com.cinchapi.concourse.server.io.process.PrematureShutdownHandler;
import com.cinchapi.concourse.server.plugin.Plugin.Instruction;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.ComplexTObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.ConcurrentMaps;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

/**
 * A {@link PluginManager} is responsible for handling all things (i.e.
 * starting, stopping, etc) related to plugins.
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
                String filename = Paths.get(url.getFile()).getFileName()
                        .toString();
                if(filename.endsWith(".jar")) {
                    SYSTEM_JARS.add(filename);
                }
            }
        }
        Reflections.log = null;
    }

    /**
     * The directory of plugins that are managed by this.
     */
    private final String home;

    /**
     * A table that contains metadata about the plugins managed herewithin.
     * (endpoint_class (id) | plugin | shared_memory_path | status |
     * app_instance)
     */
    private final Table<String, PluginInfoColumn, Object> pluginInfo = HashBasedTable
            .create();

    // TODO make the plugin launcher watch the directory for changes/additions
    // and when new plugins are added, it should launch them

    /**
     * All the {@link SharedMemory streams} for which real time data updates are
     * sent.
     */
    private final Set<SharedMemory> streams = Sets.newSetFromMap(Maps
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
        this.home = directory;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                stop();
            }

        }));
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
        SharedMemory fromServer = (SharedMemory) pluginInfo.get(clazz,
                PluginInfoColumn.FROM_SERVER);
        RemoteMethodRequest request = new RemoteMethodRequest(method, creds,
                transaction, environment, args);
        ByteBuffer data0 = Serializables.getBytes(request);
        ByteBuffer data = ByteBuffer.allocate(data0.capacity() + 4);
        data.putInt(Plugin.Instruction.REQUEST.ordinal());
        data.put(data0);
        fromServer.write(ByteBuffers.rewind(data));
        ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) pluginInfo
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
     * Start the plugin manager.
     */
    public void start() {
        this.template = FileSystem.read(Resources
                .getAbsolutePath("/META-INF/PluginLauncher.tpl"));
        for (String plugin : FileSystem.getSubDirs(home)) {
            activate(plugin);
        }
    }

    /**
     * Stop the plugin manager and shutdown any managed plugins that are
     * running.
     */
    public void stop() {
        for (String id : pluginInfo.rowKeySet()) {
            JavaApp app = (JavaApp) pluginInfo.get(id,
                    PluginInfoColumn.APP_INSTANCE);
            app.destroy();
        }
        pluginInfo.clear();
    }

    /**
     * Get all the {@link Plugin plugins} in the {@code dist} and
     * {@link #launch(String, Path, Class, List) launch} them each in a separate
     * JVM.
     * 
     * @param dist the path to a distribution directory, which is a sub-
     *            directory of the {@link #home} directory.
     */
    protected void activate(String dist) {
        try {
            String lib = home + File.separator + dist + File.separator + "lib"
                    + File.separator;
            Path prefs = Paths.get(home, dist,
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
            Set<Class<?>> plugins = reflection.getSubTypesOf(parent);
            for (final Class<?> plugin : plugins) { // For each plugin, spawn a
                                                    // separate JVM
                launch(dist, prefs, plugin, classpath);
                if(realTimeParent.isAssignableFrom(plugin)) {
                    // If the #plugin extends RealTimePlugin, create another
                    // SharedMemory segment over which the PluginManager will
                    // stream Packets that contain writes.
                    String stream0 = FileSystem.tempFile();
                    SharedMemory stream = new SharedMemory(stream0);
                    ByteBuffer data0 = ByteBuffers.fromString(stream0);
                    ByteBuffer data = ByteBuffer.allocate(data0.capacity() + 4);
                    data.putInt(Instruction.MESSAGE.ordinal());
                    data.put(data0);
                    SharedMemory fromServer = (SharedMemory) pluginInfo.get(
                            plugin.getName(), PluginInfoColumn.FROM_SERVER);
                    fromServer.write(ByteBuffers.rewind(data));
                    streams.add(stream);
                }
            }

        }
        catch (IOException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Launch the {@code plugin} from {@code dist} within a separate JVM
     * configured with the specified {@code classpath} and the values from the
     * {@code prefs} file.
     * 
     * @param dist the distribution directory that contains the plugin libraries
     * @param prefs the {@link Path} to the config file
     * @param plugin the class to launch in a separate JVM
     * @param classpath the classpath for the separate JVM
     */
    private void launch(final String dist, final Path prefs,
            final Class<?> plugin, final List<String> classpath) {
        // Write an arbitrary main class that'll construct the Plugin and run it
        String launchClass = plugin.getName();
        String launchClassShort = plugin.getSimpleName();
        String fromServer = FileSystem.tempFile();
        String fromPlugin = FileSystem.tempFile();
        String source = template
                .replace("INSERT_IMPORT_STATEMENT", launchClass)
                .replace("INSERT_SERVER_LOOP", fromServer)
                .replace("INSERT_PLUGIN_LOOP", fromPlugin)
                .replace("INSERT_CLASS_NAME", launchClassShort);

        // Create an external JavaApp in which the Plugin will run. Get the
        // plugin config to size the JVM properly.
        PluginConfiguration config = Reflection.newInstance(
                StandardPluginConfiguration.class, prefs);
        long heapSize = config.getHeapSize() / BYTES_PER_MB;
        String[] options = new String[] { "-Xms" + heapSize + "M",
                "-Xmx" + heapSize + "M" };
        JavaApp app = new JavaApp(StringUtils.join(classpath,
                JavaApp.CLASSPATH_SEPARATOR), source, options);
        app.run();
        if(app.isRunning()) {
            Logger.info("Starting plugin '{}' from package '{}'", launchClass,
                    dist);
        }
        app.onPrematureShutdown(new PrematureShutdownHandler() {

            @Override
            public void run(InputStream out, InputStream err) {
                Logger.warn("Plugin '{}' unexpectedly crashed. "
                        + "Restarting now...", plugin);
                // TODO: it would be nice to just restart the same JavaApp
                // instance (e.g. app.restart();)
                launch(dist, prefs, plugin, classpath);
            }

        });

        // Store metadata about the Plugin
        String id = launchClass;
        pluginInfo.put(id, PluginInfoColumn.PLUGIN_DIST, dist);
        pluginInfo.put(id, PluginInfoColumn.FROM_SERVER, new SharedMemory(
                fromServer));
        pluginInfo.put(id, PluginInfoColumn.FROM_PLUGIN, new SharedMemory(
                fromPlugin));
        pluginInfo.put(id, PluginInfoColumn.STATUS, PluginStatus.ACTIVE);
        pluginInfo.put(id, PluginInfoColumn.APP_INSTANCE, app);
        pluginInfo.put(id, PluginInfoColumn.FROM_PLUGIN_RESPONSES,
                Maps.<AccessToken, RemoteMethodResponse> newConcurrentMap());

        // Start the event loop to process both #fromPlugin requests and
        // responses
        final SharedMemory requests = (SharedMemory) pluginInfo.get(id,
                PluginInfoColumn.FROM_PLUGIN);
        final SharedMemory responses = (SharedMemory) pluginInfo.get(id,
                PluginInfoColumn.FROM_SERVER);
        final ConcurrentMap<AccessToken, RemoteMethodResponse> fromPluginResponses = (ConcurrentMap<AccessToken, RemoteMethodResponse>) pluginInfo
                .get(id, PluginInfoColumn.FROM_PLUGIN_RESPONSES);
        Thread loop = new Thread(new Runnable() {

            @Override
            public void run() {
                ByteBuffer data;
                while ((data = requests.read()) != null) {
                    Plugin.Instruction type = ByteBuffers.getEnum(data,
                            Plugin.Instruction.class);
                    data = ByteBuffers.getRemaining(data);
                    if(type == Instruction.REQUEST) {
                        RemoteMethodRequest request = Serializables.read(data,
                                RemoteMethodRequest.class);
                        new RemoteInvocationThread(request, requests,
                                responses, this, true, fromPluginResponses)
                                .start();
                    }
                    else if(type == Instruction.RESPONSE) {
                        RemoteMethodResponse response = Serializables.read(
                                data, RemoteMethodResponse.class);
                        ConcurrentMaps.putAndSignal(fromPluginResponses,
                                response.creds, response);
                    }
                    else { // STOP
                        break;
                    }
                }

            }

        });
        loop.setDaemon(true);
        loop.start();
    }

    /**
     * The columns that are included in the {@link #pluginInfo} table.
     * 
     * @author Jeff Nelson
     */
    private enum PluginInfoColumn {
        APP_INSTANCE,
        FROM_PLUGIN,
        FROM_PLUGIN_RESPONSES,
        FROM_SERVER,
        PLUGIN_DIST,
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
