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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.JavaApp;
import com.cinchapi.concourse.server.io.process.PrematureShutdownHandler;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.base.MoreObjects;
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
     * A mapping from a standard {@link AccessToken} to the name of a plugin
     * endpoint class to a {@link PluginClient} that contains information
     * about the appropriate inbox/outbox for RPC communication between the
     * server and the plugin.
     */
    private final ConcurrentMap<AccessToken, Map<String, PluginClient>> clients = Maps
            .newConcurrentMap();

    /**
     * The directory of plugins that are managed by this.
     */
    private final String directory;

    /**
     * A table that contains metadata about the plugins managed herewithin.
     * (endpoint_class (id) | plugin | shared_memory_path | status |
     * app_instance)
     */
    private final Table<String, PluginInfoColumn, Object> pluginInfo = HashBasedTable
            .create();

    /**
     * A reference to the {@link ConcourseServer} instance that started this
     * plugin manager.
     */
    private final ConcourseServer server;

    // TODO make the plugin launcher watch the directory for changes/additions
    // and when new plugins are added, it should launch them

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
    public PluginManager(String directory, ConcourseServer server) {
        this.directory = directory;
        this.server = server;
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
    public Object invoke(String clazz, String method, List<Object> args,
            final AccessToken creds, TransactionToken transaction,
            String environment) {
        Map<String, PluginClient> pluginSessions = clients.get(creds);
        if(pluginSessions == null) {
            pluginSessions = Maps.newHashMap();
            Map<String, PluginClient> stored = clients.putIfAbsent(creds,
                    pluginSessions);
            pluginSessions = MoreObjects.firstNonNull(stored, pluginSessions);
        }
        PluginClient session = pluginSessions.get(clazz);
        if(session == null) {
            // When a client first interacts with a plugin, we must instruct
            // that plugin to create new communication channels (e.g. inbox and
            // outbox) to be used exclusively between the server and the client.
            session = new PluginClient(creds, FileSystem.tempFile(),
                    FileSystem.tempFile());
            Plugin.Instruction instruction = Plugin.Instruction.CONNECT_CLIENT;
            ByteBuffer sessionData = Serializables.getBytes(session);
            ByteBuffer message = ByteBuffer
                    .allocate(sessionData.capacity() + 4);
            message.putInt(instruction.ordinal());
            message.put(ByteBuffers.rewind(sessionData));
            SharedMemory broadcast = (SharedMemory) pluginInfo.get(clazz,
                    PluginInfoColumn.SHARED_MEMORY);
            broadcast.write(ByteBuffers.rewind(message));
            broadcast.read(); // wait for acknowledgement from the plugin that
                              // the client was created
            pluginSessions.put(clazz, session);

            // Setup infrastructure to handle rpc between server and plugin
            Object invocationSource = server;
            SharedMemory incoming = new SharedMemory(session.outbox);
            SharedMemory outgoing = new SharedMemory(session.inbox);
            session.localInbox = incoming; // for plugin -> server requests
            session.localOutbox = outgoing; // for server -> plugin requests
            boolean useLocalThriftArgs = true;

            // Create a thread that listens for messages from the plugin
            // requesting the invocation of a server method.
            RemoteInvocationListenerThread listener = new RemoteInvocationListenerThread(
                    invocationSource, clients.keySet(), incoming, outgoing,
                    session.creds, useLocalThriftArgs);
            listener.start();
        }

        // Place an outgoing message to the plugin for the remote method
        // invocation.
        List<TObject> targs = Lists.newArrayListWithCapacity(args.size());
        for (Object arg : args) {
            targs.add(Convert.javaToThrift(arg));
        }
        RemoteMethodInvocation invocation = new RemoteMethodInvocation(method,
                creds, transaction, environment, targs);
        ByteBuffer data = Serializables.getBytes(invocation);
        session.localOutbox.write(ByteBuffers.rewind(data));
        ByteBuffer resp = session.localOutbox.read();
        RemoteMethodResponse response = Serializables.read(
                ByteBuffers.rewind(resp), RemoteMethodResponse.class);
        Object ret = Convert.thriftToJava(response.response);
        return ret;
    }

    /**
     * Cleanup after the user session represented by {@code creds} perform a
     * logout.
     * 
     * @param creds
     */
    public void onSessionEnd(AccessToken creds) {
        Map<String, PluginClient> connected = clients.remove(creds);
        if(connected != null) {
            for (Map.Entry<String, PluginClient> info : connected.entrySet()) {
                String plugin = info.getKey();
                PluginClient session = info.getValue();
                SharedMemory broadcast = (SharedMemory) pluginInfo.get(plugin,
                        PluginInfoColumn.SHARED_MEMORY);
                Plugin.Instruction instruction = Plugin.Instruction.DISCONNECT_CLIENT;
                ByteBuffer sessionData = Serializables.getBytes(session);
                ByteBuffer message = ByteBuffer
                        .allocate(sessionData.capacity() + 4);
                message.putInt(instruction.ordinal());
                message.put(ByteBuffers.rewind(sessionData));
                broadcast.write(message);
                broadcast.read(); // wait for confirmation of client disconnect
            }
        }
    }

    /**
     * Start the plugin manager.
     */
    public void start() {
        this.template = FileSystem.read(Resources
                .getAbsolutePath("/META-INF/PluginLauncher.tpl"));
        for (String plugin : FileSystem.getSubDirs(directory)) {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void activate(String pluginDist) {
        try {
            String lib = directory + File.separator + pluginDist
                    + File.separator + "lib" + File.separator;
            Path prefs = Paths.get(directory, pluginDist,
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
            Reflections reflection = new Reflections(new ConfigurationBuilder()
                    .addClassLoader(loader).addUrls(
                            ClasspathHelper.forClassLoader(loader)));
            Set<Class<?>> plugins = reflection.getSubTypesOf(parent);
            for (final Class<?> plugin : plugins) { // For each plugin, spawn a
                                                    // separate JVM
                launch(pluginDist, prefs, plugin, classpath);
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
        String launchClass = plugin.getName();
        String launchClassShort = plugin.getSimpleName();
        String sharedMemoryPath = FileSystem.tempFile();
        String source = template
                .replace("INSERT_IMPORT_STATEMENT", launchClass)
                .replace("INSERT_SHARED_MEMORY_PATH", sharedMemoryPath)
                .replace("INSERT_CLASS_NAME", launchClassShort);

        // Get the plugin config to size the JVM properly
        PluginConfiguration config = Reflection.newInstance(
                StandardPluginConfiguration.class, prefs);
        long heapSize = config.getHeapSize() / BYTES_PER_MB;
        String[] options = new String[] { "-Xms" + heapSize + "M",
                "-Xmx" + heapSize + "M" };
        JavaApp app = new JavaApp(StringUtils.join(classpath,
                JavaApp.CLASSPATH_SEPARATOR), source, options);
        app.run();
        if(app.isRunning()) {
            Logger.info("Starting plugin '{}' from package '{}'",
                    launchClass, dist);
        }
        app.onPrematureShutdown(new PrematureShutdownHandler() {

            @Override
            public void run(InputStream out, InputStream err) {
                Logger.warn("Plugin '{}' unexpectedly crashed. "
                        + "Restarting now...", plugin);
                launch(dist, prefs, plugin, classpath);

            }

        });
        String id = launchClass;
        pluginInfo.put(id, PluginInfoColumn.PLUGIN_DIST, dist);
        pluginInfo.put(id, PluginInfoColumn.SHARED_MEMORY, new SharedMemory(
                sharedMemoryPath));
        pluginInfo.put(id, PluginInfoColumn.STATUS, PluginStatus.ACTIVE);
        pluginInfo.put(id, PluginInfoColumn.APP_INSTANCE, app);
    }

    /**
     * The columns that are included in the {@link #pluginInfo} table.
     * 
     * @author Jeff Nelson
     */
    private enum PluginInfoColumn {
        APP_INSTANCE, PLUGIN_DIST, SHARED_MEMORY, STATUS
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
