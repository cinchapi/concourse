/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.server.http;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.util.Logger;
import org.cinchapi.concourse.util.Reflection;
import org.reflections.Reflections;
import org.slf4j.LoggerFactory;

import spark.Spark;
import ch.qos.logback.classic.Level;

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
    public static HttpServer create(ConcourseServer concourseServer, int port) {
        return new HttpServer(concourseServer, port, "public");
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
    public static HttpServer create(ConcourseServer concourseServer, int port,
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
                Logger.info("HTTP server is turned OFF");
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
     * A reference ConcourseServer instance.
     */
    private final ConcourseServer concourseServer;

    /**
     * Construct a new instance.
     * 
     * @param port
     * @param staticFileLocation
     */
    private HttpServer(ConcourseServer concourseServer, int port,
            String staticFileLocation) {
        this.port = port;
        this.staticFileLocation = staticFileLocation;
        this.concourseServer = concourseServer;
    }

    /**
     * Start the server.
     */
    public void start() {
        if(running.compareAndSet(false, true)) {
            Spark.setPort(port);
            Spark.staticFileLocation(staticFileLocation);

            // Register all the routers and listen for any requests
            Reflections reflections = new Reflections(
                    "org.cinchapi.concourse.server.http.routers");
            for (Class<? extends Router> router : reflections
                    .getSubTypesOf(Router.class)) {
                Reflection.newInstance(router, concourseServer).init();
            }
            Logger.info("HTTP server is listening for requests on port {}", port);
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
