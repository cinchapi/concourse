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

import java.lang.reflect.Field;
import java.util.List;

import com.google.common.base.CaseFormat;
import com.google.common.base.Throwables;

import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.util.Strings;

import spark.Filter;
import spark.Request;
import spark.Response;
import spark.Spark;

/**
 * A {@link Router} is responsible for defining accessible routes and serving
 * an {@link AbstractView} or {@link Resource}.
 * <p>
 * The name of the Router is used for determining the absolute path to prepend
 * to the relative paths defined for each {@link #init() route}. The name of the
 * class is converted from upper camelcase to lowercase where each word boundary
 * is separated by a forward slash (/) and the words "Router" and "Index" are
 * stripped.
 * </p>
 * <p>
 * For example, a class named {@code HelloWorldRouter} will have each of its
 * {@link #init()} prepended with {@code /hello/world/}.
 * <p>
 * <p>
 * {@link Endpoint Endpoints} are defined in a Router using instance variables.
 * The name of the variable is used to determine the path of the endpoint. For
 * example, an Endpoint instance variable named {@code get$Arg1Foo$Arg2}
 * corresponds to the path {@code GET /:arg1/foo/:arg2}. Each endpoint must
 * respond to one of the HTTP verbs (GET, POST, PUT, DELETE) and serve either a
 * {@link View} or {@link Resource}.
 * <p>
 * You may define multiple endpoints that process the same path as long as each
 * pone responds to a different HTTP verb (i.e. you may have GET /path/to/foo
 * and POST /path/to/foo). On the other hand, you may not define two endpoints
 * that respond to the same HTTP Verb, even if they serve different kinds of
 * data (i.e. you cannot have GET /path/to/foo that serves a View and GET
 * /path/to/foo that serves an Resource).
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class Router {

    /**
     * A reference to the {@link ConcourseServer} instance.
     */
    protected final ConcourseServer concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public Router(ConcourseServer concourse) {
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
     * Register all of the defined endpoints.
     */
    public final void init() {
        try {
            for (Field field : this.getClass().getDeclaredFields()) {
                if(Endpoint.class.isAssignableFrom(field.getType())
                        && (field.getName().startsWith("get")
                                || field.getName().startsWith("post")
                                || field.getName().startsWith("put")
                                || field.getName().startsWith("delete") || field
                                .getName().startsWith("upsert"))) {
                    List<String> args = Strings.splitCamelCase(field.getName());
                    String action = args.remove(0);
                    String path = namespace;
                    if(args.isEmpty()) {
                        path += "/";
                    }
                    else {
                        boolean var = false;
                        StringBuilder sb = new StringBuilder();
                        for (String component : args) {
                            if(component.equals("$")) {
                                var = true;
                                continue;
                            }
                            sb.append("/");
                            if(var) {
                                sb.append(":");
                            }
                            sb.append(component.toLowerCase());
                        }
                        path += sb.toString();
                    }
                    final Endpoint endpoint = (Endpoint) field.get(this);
                    endpoint.setPath(path);
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
                    else if(action.equals("before")) {
                        Spark.before(new Filter() {

                            @Override
                            public void handle(Request request,
                                    Response response) {
                                endpoint.handle(request, response);

                            }

                        });
                    }
                    else if(action.equals("after")) {
                        Spark.after(new Filter() {

                            @Override
                            public void handle(Request request,
                                    Response response) {
                                endpoint.handle(request, response);

                            }

                        });
                    }
                    else {
                        continue;
                    }
                }
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

}
