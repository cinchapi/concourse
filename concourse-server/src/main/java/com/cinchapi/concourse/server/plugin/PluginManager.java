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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.beust.jcommander.internal.Sets;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.JavaApp;
import com.cinchapi.concourse.server.io.process.PrematureShutdownHandler;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Resources;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class PluginManager {

    private enum PluginStatus {
        ACTIVE;
    }

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
     * The template to use when creating {@link JavaApp external java processes}
     * to run the plugin code.
     */
    private String template;

    /**
     * The directory of plugins that are managed by this.
     */
    private final String directory;

    /**
     * A table that contains metadata about the plugins managed herewithin.
     * (id | plugin | endpoint_class | shared_memory_path | status |
     * app_instance)
     */
    private final Table<Long, String, Object> pluginInfo = HashBasedTable
            .create();

    // TODO make the plugin launcher watch the directory for changes/additions
    // and when new plugins are added, it should launch them

    /**
     * Construct a new instance.
     * 
     * @param directory
     */
    public PluginManager(String directory) {
        this.directory = directory;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                stop();
            }

        }));
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
        for (long id : pluginInfo.rowKeySet()) {
            JavaApp app = (JavaApp) pluginInfo.get(id, "app_instance");
            app.destroy();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void activate(String plugin) {
        try {
            String lib = directory + File.separator + plugin + File.separator
                    + "lib" + File.separator;
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
            ConfigurationBuilder config = new ConfigurationBuilder();
            config.addClassLoader(loader);
            config.addUrls(ClasspathHelper.forClassLoader(loader));
            Reflections reflection = new Reflections(config);
            Set<Class<?>> endpoints = reflection.getSubTypesOf(parent);
            for (Class<?> endpoint : endpoints) { // For each endpoint, spawn a
                                                  // separate JVM
                String launchClass = endpoint.getName();
                String launchClassShort = endpoint.getSimpleName();
                String sharedMemoryPath = FileSystem.tempFile();
                String source = template
                        .replace("INSERT_IMPORT_STATEMENT", launchClass)
                        .replace("INSERT_SHARED_MEMORY_PATH", sharedMemoryPath)
                        .replace("INSERT_CLASS_NAME", launchClassShort);
                // TODO get plugin config so we know how much memory and stuff
                // to launch each endpoint with
                JavaApp app = new JavaApp(StringUtils.join(classpath,
                        JavaApp.CLASSPATH_SEPARATOR), source);
                app.run();
                if(app.isRunning()) {
                    Logger.info("Activated endpoint '{}' in plugin '{}'",
                            launchClass, plugin);
                }
                app.onPrematureShutdown(new PrematureShutdownHandler() {

                    @Override
                    public void run(InputStream out, InputStream err) {
                        System.out.println("The app is DEAD!!!");
                        // TODO restart app

                    }

                });
                long id = Time.now();
                pluginInfo.put(id, "plugin", plugin);
                pluginInfo.put(id, "endpoint_class", launchClass);
                pluginInfo.put(id, "shared_memory_path", sharedMemoryPath);
                pluginInfo.put(id, "status", PluginStatus.ACTIVE);
                pluginInfo.put(id, "app_instance", app);
            }

        }
        catch (IOException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void main(String... args) {
        PluginManager mgr = new PluginManager("plugins");
        mgr.start();
    }

}
