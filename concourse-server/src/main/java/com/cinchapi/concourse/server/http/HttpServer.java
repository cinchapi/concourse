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
package com.cinchapi.concourse.server.http;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reflections.Reflections;
import org.slf4j.LoggerFactory;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;
import ch.qos.logback.classic.Level;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.errors.HttpError;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.gson.JsonObject;

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
     * @param concourseServer reference to the ConcourseServer
     * @param port
     * @return the HttpServer
     */
    public static HttpServer create(ConcourseServer concourseServer, int port) {
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
     * A reference to the {@link ConcourseServer backend} that is associated
     * with this {@link HttpServer}. Typically, the runtime embeds the server.
     */
    private final ConcourseServer concourse;

    /**
     * Construct a new instance.
     * 
     * @param port
     * @param staticFileLocation
     */
    private HttpServer(ConcourseServer concourse, int port,
            String staticFileLocation) {
        this.port = port;
        this.staticFileLocation = staticFileLocation;
        this.concourse = concourse;
    }

    /**
     * Initialize a {@link EndpointContainer container} by registering all of
     * its
     * endpoints.
     * 
     * @param container the {@link EndpointContainer} to initialize
     */
    private static void initialize(EndpointContainer container) {
        for (final Endpoint endpoint : container.endpoints()) {
            String action = endpoint.getAction();
            Route route = new Route(endpoint.getPath()) {

                @Override
                public Object handle(Request request, Response response) {
                    response.type(endpoint.getContentType().toString());
                    // The HttpRequests preprocessor assigns attributes to the
                    // request in order for the Endpoint to make calls into
                    // ConcourseServer.
                    AccessToken creds = (AccessToken) request
                            .attribute(GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE);
                    String environment = MoreObjects
                            .firstNonNull(
                                    (String) request
                                            .attribute(GlobalState.HTTP_ENVIRONMENT_ATTRIBUTE),
                                    GlobalState.DEFAULT_ENVIRONMENT);
                    String fingerprint = (String) request
                            .attribute(GlobalState.HTTP_FINGERPRINT_ATTRIBUTE);

                    // Check basic authentication: is an AccessToken present and
                    // does the fingerprint match?
                    if((boolean) request
                            .attribute(GlobalState.HTTP_REQUIRE_AUTH_ATTRIBUTE)
                            && creds == null) {
                        halt(401);
                    }
                    if(!Strings.isNullOrEmpty(fingerprint)
                            && !fingerprint.equals(HttpRequests
                                    .getFingerprint(request))) {
                        Logger.warn(
                                "Request made with mismatching fingerprint. Expecting {} but got {}",
                                HttpRequests.getFingerprint(request),
                                fingerprint);
                        halt(401);
                    }
                    TransactionToken transaction = null;
                    try {
                        Long timestamp = Longs
                                .tryParse((String) request
                                        .attribute(GlobalState.HTTP_TRANSACTION_TOKEN_ATTRIBUTE));
                        transaction = creds != null && timestamp != null ? new TransactionToken(
                                creds, timestamp) : transaction;
                    }
                    catch (NullPointerException e) {}
                    try {
                        return endpoint.serve(request, response, creds,
                                transaction, environment);
                    }
                    catch (Exception e) {
                        if(e instanceof HttpError) {
                            response.status(((HttpError) e).getCode());
                        }
                        else if(e instanceof SecurityException
                                || e instanceof java.lang.SecurityException) {
                            response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
                            response.status(401);
                        }
                        else if(e instanceof IllegalArgumentException) {
                            response.status(400);
                        }
                        else {
                            response.status(500);
                            Logger.error("", e);
                        }
                        JsonObject json = new JsonObject();
                        json.addProperty("error", e.getMessage());
                        return json.toString();
                    }
                }

            };
            if(action.equals("get")) {
                Spark.get(route);
            }
            else if(action.equals("post")) {
                Spark.post(route);
            }
            else if(action.equals("put")) {
                Spark.put(route);
            }
            else if(action.equals("delete")) {
                Spark.delete(route);
            }
            else if(action.equals("upsert")) {
                Spark.post(route);
                Spark.put(route);
            }
            else if(action.equals("options")) {
                Spark.options(route);
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

            // Register all the EndpointContainers and listen for any requests
            Reflections reflections = new Reflections(
                    "com.cinchapi.concourse.server.http.router");
            Set<EndpointContainer> weighted = Sets.newTreeSet();
            for (Class<? extends EndpointContainer> container : reflections
                    .getSubTypesOf(EndpointContainer.class)) {
                EndpointContainer instance = Reflection.newInstance(container,
                        concourse);
                weighted.add(instance);
            }
            for (EndpointContainer instance : weighted) {
                initialize(instance);
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
