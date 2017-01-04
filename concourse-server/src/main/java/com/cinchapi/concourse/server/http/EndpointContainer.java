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

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;

/**
 * An {@link EndpointContainer} is one that exposes functionality via HTTP
 * {@link Endpoint endpoints}.
 * 
 * <p>
 * The name of the EndpointContainer is used for determining the absolute path
 * to prepend to the relative paths defined for each {@link Endpoint}. The name
 * of the class is converted from upper camelcase to lowercase where each word
 * boundary is separated by a forward slash (/) and the words "Router" and
 * "Index" are stripped.
 * </p>
 * <p>
 * For example, a class named {@code com.company.module.HelloWorldRouter} will
 * have each of its {@link Endpoint endpoints} prepended with
 * {@code /com/company/module/hello/world/}.
 * <p>
 * <p>
 * {@link Endpoint Endpoints} are defined in an EndpointContainer using instance
 * variables. The name of the variable is used to determine the relative path of
 * the endpoint. For example, an Endpoint instance variable named
 * {@code get$Arg1Foo$Arg2} corresponds to the path {@code GET /:arg1/foo/:arg2}
 * relative to the path defined by the EndpointContainer. Each endpoint must
 * respond to one of the HTTP verbs (GET, POST, PUT, DELETE) and serve some
 * payload.
 * <p>
 * You may define multiple endpoints that process the same URI as long as each
 * one responds to a different HTTP verb (i.e. you may have GET /path/to/foo and
 * POST /path/to/foo).
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class EndpointContainer implements
        Comparable<EndpointContainer> {

    /**
     * Given a list of arguments (as defined by the spec for declaring
     * {@link Endpoint} objects), generate a string that can use to create
     * the appropriate Spark routes.
     * 
     * @param args a list of arguments
     * @return the path to {@link Endpoint#setPath(String) assign}
     */
    @VisibleForTesting
    protected static String buildSparkPath(List<String> args) {
        if(args.isEmpty()) {
            return "";
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
                    var = false;
                }
                sb.append(component.toLowerCase());
            }
            sb.deleteCharAt(0);
            return sb.toString();
        }
    }

    /**
     * Given a fully qualified class {@code name}, return the canonical
     * namespace that should be used as a prefix when referring to the
     * container's
     * classes.
     * 
     * <p>
     * The namespace is instrumental for properly constructing the URI where the
     * container's functionality lives.
     * </p>
     * 
     * @param name a fully qualified class name
     * @return the canonical namespace to use when constructing the URI
     */
    @VisibleForTesting
    protected static String getCanonicalNamespace(String name) {
        return getCanonicalNamespace(RoutingKey.forName(name));
    }

    /**
     * Given a {@link RoutingKey}, return the canonical namespace that should be
     * used as a prefix when referring to the container's classes.
     * 
     * <p>
     * The namespace is instrumental for properly constructing the URI where the
     * container's functionality lives.
     * </p>
     * 
     * @param id a {@link RoutingKey}
     * @return the canonical namespace to use when constructing the URI
     */
    private static String getCanonicalNamespace(RoutingKey id) {
        String namespace;
        if(id.group.equals("com.cinchapi")) {
            if(id.module.equals("server")) {
                namespace = id.cls;
            }
            else {
                namespace = Strings.join('/', id.module, id.cls);
            }
        }
        else {
            namespace = Strings.join('/', id.group, id.module, id.cls);
        }
        namespace = namespace.replace("Router", "");
        namespace = namespace.replace("Index", "");
        namespace = namespace.replaceAll("\\.", "/");
        namespace = namespace.replaceAll("_", "/");
        namespace = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN,
                namespace);
        namespace = namespace.replaceAll("/-", "/");
        namespace = Strings.ensureStartsWith(namespace, "/");
        namespace = Strings.ensureEndsWith(namespace, "/");
        return namespace;
    }

    /**
     * A reference to the {@link ConcourseServer backend} where this container
     * is
     * registered.
     */
    protected final ConcourseServer concourse;

    /**
     * The namespace is prepended to the relative paths for every
     * {@link Endpoint}.
     */
    protected final String namespace = getCanonicalNamespace(getRoutingKey());

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected EndpointContainer(ConcourseServer concourse) {
        this.concourse = concourse;
    }

    @Override
    public int compareTo(EndpointContainer other) {
        int p0 = getWeight();
        int p1 = other.getWeight();
        if(this == other) {
            return 0;
        }
        else if(p0 == p1) {
            int c = getClass().getSimpleName().compareTo(
                    other.getClass().getSimpleName());
            if(c != 0) {
                return c;
            }
            else {
                // If all other comparisons indicate that the containers have
                // the same weight and same name, then just randomly sort them.
                return Random.getInt() % 2 == 0 ? 1 : -1;
            }
        }
        else {
            return p0 > p1 ? 1 : -1;
        }
    }

    /**
     * Return an {@link Iterable iterable} collection of all the
     * {@link Endpoint endpoints} that are defined in this container.
     * 
     * @return all the defined endpoints
     */
    public final Iterable<Endpoint> endpoints() {
        return new Iterable<Endpoint>() {

            private final Field[] fields = EndpointContainer.this.getClass()
                    .getDeclaredFields();
            private int i = 0;

            @Override
            public Iterator<Endpoint> iterator() {
                return new AdHocIterator<Endpoint>() {

                    @Override
                    protected Endpoint findNext() {
                        if(i >= fields.length) {
                            return null;
                        }
                        else {
                            Field field = fields[i];
                            String name = field.getName();
                            ++i;
                            if(Endpoint.class.isAssignableFrom(field.getType())) {
                                Endpoint callable = Reflection.getCasted(field,
                                        EndpointContainer.this);
                                String action = callable.getAction();
                                String path = callable.getPath();
                                if(action == null
                                        && path == null
                                        && (name.startsWith("get")
                                                || name.startsWith("post")
                                                || name.startsWith("put")
                                                || name.startsWith("delete")
                                                || name.startsWith("upsert") || name
                                                    .startsWith("options"))) {
                                    List<String> args = Strings
                                            .splitCamelCase(field.getName());
                                    action = args.remove(0);
                                    path = buildSparkPath(args);
                                }
                                path = Strings.joinSimple(namespace, path);
                                Reflection.set("action", action, callable);
                                Reflection.set("path", path, callable);
                                return callable;
                            }
                            else {
                                return findNext();
                            }
                        }
                    }
                };
            }
        };
    }

    /**
     * Return the appropriate {@link RoutingKey}. Subclasses may wish to
     * override
     * to provide custom naming functionality.
     * 
     * @return the {@link RoutingKey}
     */
    protected final RoutingKey getRoutingKey() {
        return RoutingKey.forClass(this.getClass());
    }

    /**
     * Return the relative weight for this {@link EndpointContainer container}.
     * Weights
     * are used to determine the order in which {@link EndpointContainer
     * containers} from
     * the same bundle are weighted. This is important because routes are
     * matched in the order in which they are registered.
     * 
     * <p>
     * A larger weight means that the routes herwithin will be registered later
     * (e.g. larger weights sink to the bottom). {@link EndpointContainer
     * Plugins} that have the same weight are registered in a random order that
     * may change between JVM invocations.
     * </p>
     * 
     * @return the container weight
     */
    protected int getWeight() {
        return 0;
    }

}
