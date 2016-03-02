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
package com.cinchapi.concourse.server.http;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reflections.Reflections;
import org.slf4j.LoggerFactory;

import spark.Spark;
import ch.qos.logback.classic.Level;

import com.cinchapi.concourse.plugin.ConcourseRuntime;
import com.cinchapi.concourse.plugin.http.HttpCallable;
import com.cinchapi.concourse.plugin.http.HttpPlugin;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Reflection;
import com.google.common.base.Throwables;

/**
 * An server that can handle HTTP requests and delegate calls to a
 * {@link ConcourseServer} instance.
 * 
 * @author Jeff Nelson
 */
public class HttpServer {

    /**
     * Return an {@link HttpServer} that listens on the specified {@code port}
     * and serves static files from the default location.
     * 
     * @param - reference to the ConcourseServer
     * @param port
     * @return the HttpServer
     */
    public static HttpServer create(ConcourseRuntime concourseServer, int port) {
        return new HttpServer(concourseServer, port, "/public");
    }

    /**
     * Return an {@link HttpServer} that listens on the specified {@code port}
     * and serves static files from the {@link staticFileLocation}.
     * 
     * @param concourseServer - reference to the ConcourseServer
     * @param port
     * @param staticFileLocation
     * @return the HttpServer
     */
    public static HttpServer create(ConcourseRuntime concourseServer, int port,
            String staticFileLocation) {
        return new HttpServer(concourseServer, port, staticFileLocation);
    }

    /**
     * Return an {@link HttpServer} that doesn't do anything.
     * 
     * @return the HttpServer
     */
    public static HttpServer disabled() {
        return new HttpServer(null, 0, "") {

            @Override
            public void start() {
                Logger.info("HTTP Server disabled");
            };

            @Override
            public void stop() {};

        };

    }

    // Turn off logging from third-party code
    static {
        Reflections.log = null;
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("org.eclipse.jetty")).setLevel(Level.OFF);
    }

    /**
     * The port on which the HTTP/S server listens.
     */
    private final int port;

    /**
     * A flag that indicates if the HttpServer is running or not.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * The location of any static files (i.e. templates, images, etc) that are
     * referenced by any Views.
     */
    private final String staticFileLocation;

    /**
     * A reference to the {@link ConcourseRuntime runtime} that is associated
     * with this {@link HttpServer}. Typically, the runtime embeds the server.
     */
    private final ConcourseRuntime concourse;

    /**
     * Construct a new instance.
     * 
     * @param port
     * @param staticFileLocation
     */
    private HttpServer(ConcourseRuntime concourse, int port,
            String staticFileLocation) {
        this.port = port;
        this.staticFileLocation = staticFileLocation;
        this.concourse = concourse;
    }

    /**
     * Initialize a {@link HttpPlugin plugin} by registering all of its
     * endpoints.
     * 
     * @param plugin the {@link HttpPlugin} to initialize
     */
    private static void initialize(HttpPlugin plugin) {
        for (HttpCallable callable : plugin.endpoints()) {
            String action = callable.getAction();
            Endpoint endpoint = (Endpoint) callable;
            if(action.equals("get")) {
                Spark.get(endpoint);
            }
            else if(action.equals("post")) {
                Spark.post(endpoint);
            }
            else if(action.equals("put")) {
                Spark.put(endpoint);
            }
            else if(action.equals("delete")) {
                Spark.delete(endpoint);
            }
            else if(action.equals("upsert")) {
                Spark.post(endpoint);
                Spark.put(endpoint);
            }
        }
    }

    /**
     * Start the server.
     */
    public void start() {
        if(running.compareAndSet(false, true)) {
            Spark.setPort(port);
            Spark.staticFileLocation(staticFileLocation);

            // Register all the HttpPlugins and listen for any requests
            Reflections reflections = new Reflections(
                    "com.cinchapi.concourse.server.http.plugin");
            for (Class<? extends HttpPlugin> plugin : reflections
                    .getSubTypesOf(HttpPlugin.class)) {
                HttpPlugin thePlugin = Reflection
                        .newInstance(plugin, concourse);
                initialize(thePlugin);
            }
            Logger.info("HTTP Server enabled on port {}", port);
        }
    }

    /**
     * Stop the server
     */
    public void stop() {
        if(running.compareAndSet(true, false)) {
            try {
                Method stop = Spark.class.getDeclaredMethod("stop");
                Method clearRoutes = Spark.class
                        .getDeclaredMethod("clearRoutes");
                stop.setAccessible(true);
                clearRoutes.setAccessible(true);
                stop.invoke(null);
                clearRoutes.invoke(null);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }

        }
    }

}
