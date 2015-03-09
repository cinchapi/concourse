/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.server.http;

import com.google.common.base.CaseFormat;

import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.vendor.spark.Filter;
import org.cinchapi.vendor.spark.Request;
import org.cinchapi.vendor.spark.Response;
import org.cinchapi.vendor.spark.Spark;

/**
 * A {@link Router} is responsible for defining accessible routes and serving
 * an {@link AbstractView} or {@link Endpoint}.
 * <p>
 * The name of the Router is used for determining the absolute path to prepend
 * to the relative paths defined for each {@link #routes() route}. The name of
 * the class is converted from upper camelcase to lowercase where each word
 * boundary is separated by a forward slash (/) and the words "Router" and
 * "Index" are stripped.
 * </p>
 * <p>
 * For exampe, a class named {@code HelloWorldRouter} will have each of its
 * {@link #routes()} prepended with {@code /hello/world/}.
 * <p>
 * 
 * @author jnelson
 */
public abstract class Router {
    
    protected final ConcourseServer concourse;
    
    public Router(ConcourseServer concourse){
        this.concourse = concourse;
    }

    /**
     * The namespace is the name of the class without the word "Router". The
     * namespace is used to handle the appropriate re-writing for all the routes
     * that are served by this {@link Router}.
     */
    private final String namespace = CaseFormat.UPPER_CAMEL.to(
            CaseFormat.LOWER_UNDERSCORE,
            this.getClass().getSimpleName().replace("Router", "")
                    .replace("Index", "")).replace("_", "/");

    /**
     * Run this {@code routine} after any of the routes defined in this
     * {@link Router} are run.
     * 
     * @param routine
     */
    public void after(final Routine routine) {
        routine.prepend(namespace);
        Spark.after(new Filter(routine.getRoutePath()) {

            @Override
            public void handle(Request request, Response response) {
                routine.handle(request, response);
            }

        });
    }

    /**
     * Run this {@code routine} before any of the routes defined in this
     * {@link Router} are run.
     * 
     * @param routine
     */
    public void before(final Routine routine) {
        routine.prepend(namespace);
        Spark.before(new Filter(routine.getRoutePath()) {

            @Override
            public void handle(Request request, Response response) {
                routine.handle(request, response);

            }

        });

    }

    /**
     * Perform a DELETE request and process the {@link RewritableRoute}.
     * 
     * @param route
     */
    public void delete(final RewritableRoute route) {
        route.prepend(namespace);
        Spark.delete(route);
    }

    /**
     * Perform a GET request and process the {@link RewritableRoute}.
     * 
     * @param route
     */
    public void get(final RewritableRoute route) {
        route.prepend(namespace);
        Spark.get(route);
    }

    /**
     * Perform a POST request and process the {@link RewritableRoute}.
     * 
     * @param route
     */
    public void post(final RewritableRoute route) {
        route.prepend(namespace);
        Spark.post(route);
    }

    /**
     * Perform a PUT request and process the {@link RewritableRoute}.
     * 
     * @param route
     */
    public void put(final RewritableRoute route) {
        route.prepend(namespace);
        Spark.put(route);
    }

    /**
     * Define and implement the routes that are handled by this {@link Router}.
     * Each route must respond to one of the HTTP verbs (GET, POST, PUT, DELETE)
     * and serve either a {@link View} or {@link Endpoint}.
     * <p>
     * You may define multiple routes that process the same path as long as each
     * route responds to a different HTTP verb (i.e. you may have GET
     * /path/to/foo and POST /path/to/foo). On the other hand, you may not
     * define two routes that respond to the same HTTP Verb, even if they serve
     * different kinds of resources (i.e. you cannot have GET /path/to/foo that
     * serves a View and GET /path/to/foo that serves an Endpoint).
     * </p>
     * <p>
     * <h2>Defining Views</h2>
     * A {@link View} specifies the template to serve to the client and the data
     * to supply. Views are defined as follows:
     * 
     * <pre>
     * get(new View(&quot;/path/to/foo&quot;) {
     * 
     *     protected String template() {
     *         return &quot;foo.mustache&quot;;
     *     }
     * 
     *     protected Map&lt;String, Object&gt; data() {
     *         Map&lt;String, Object&gt; data = Maps.newHashMap();
     *         // populate data
     *         return data;
     *     }
     * 
     * });
     * </pre>
     * 
     * The router will fill in variables defined in the specified template with
     * the appropriate values from {@code data()}.
     * <p>
     * <p>
     * <h3>Defining Endpoints</h2> An {@link Endpoint} returns a json payload in
     * response to an HTTP request. Endpoints are defined as follows:
     * 
     * <pre>
     * </pre>
     * </p>
     */
    public abstract void routes();

}
