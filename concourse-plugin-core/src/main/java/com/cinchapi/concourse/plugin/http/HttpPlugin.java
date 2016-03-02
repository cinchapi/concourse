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
package com.cinchapi.concourse.plugin.http;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.concourse.plugin.ConcourseRuntime;
import com.cinchapi.concourse.plugin.Plugin;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * An {@link HttpPlugin} is one that exposes functionality via HTTP
 * {@link HttpCallable endpoints}.
 * 
 * A {@link Router} is responsible for defining accessible routes and serving
 * a {@link UIState} or {@link Resource}.
 * <p>
 * The name of the Router is used for determining the absolute path to prepend
 * to the relative paths defined for each {@link #init() endpoint}. The name of
 * the class is converted from upper camelcase to lowercase where each word
 * boundary is separated by a forward slash (/) and the words "Router" and
 * "Index" are stripped.
 * </p>
 * <p>
 * For example, a class named {@code HelloWorldRouter} will have each of its
 * {@link #init() endpoints} prepended with {@code /hello/world/}.
 * <p>
 * <p>
 * {@link Endpoint Endpoints} are defined in a Router using instance variables.
 * The name of the variable is used to determine the relative path of the
 * endpoint. For example, an Endpoint instance variable named
 * {@code get$Arg1Foo$Arg2} corresponds to the path {@code GET /:arg1/foo/:arg2}
 * relative to the path defined by the Router. Each endpoint must respond to one
 * of the HTTP verbs (GET, POST, PUT, DELETE) and serve either a {@link UIState}
 * or {@link Resource}.
 * <p>
 * You may define multiple endpoints that process the same path as long as each
 * one responds to a different HTTP verb (i.e. you may have GET /path/to/foo and
 * POST /path/to/foo). On the other hand, you may not define two endpoints that
 * respond to the same HTTP Verb, even if they serve different kinds of data
 * (i.e. you cannot have GET /path/to/foo that serves a {@link UIState} and GET
 * /path/to/foo that serves a {@link Resource}).
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class HttpPlugin extends Plugin {

    /**
     * An designation to the level of aliasing that a plugin receives.
     * 
     * @author Jeff Nelson
     */
    private enum AliasLevel {
        NONE, MODULE, FULL
    }

    /**
     * A mapping from {domain}.{company}.{module} to the level of aliasing to
     * which plugins housed in that namespace are entitled.
     */
    private static final Map<String, AliasLevel> ALIAS_LEVELS = Maps
            .newHashMap();
    static {
        ALIAS_LEVELS.put("com.cinchapi.concourse", AliasLevel.FULL);
    }

    /**
     * Given a fully qualified class {@code name}, return the canonical
     * namespace that should be used as a prefix when referring to the plugin's
     * classes.
     * 
     * <p>
     * The namespace is instrumental for properly constructing the URI where the
     * plugin's functionality lives.
     * </p>
     * 
     * @param name a fully qualified class name
     * @return the canonical namespace to use when constructing the URI
     */
    @VisibleForTesting
    protected static String getCanonicalNamespace(String name) {
        String[] toks = name.split("\\.");
        Preconditions.checkArgument(toks.length >= 4,
                "%s is not a valid plugin name. The correct "
                        + "format is {domain}.{company}.{module}.{...(.)}"
                        + "{class}", name);
        String domain = toks[0];
        String company = toks[1];
        String module = toks[2];
        String clazz = toks[toks.length - 1];
        AliasLevel aliasLevel = MoreObjects.firstNonNull(
                ALIAS_LEVELS.get(Strings.join('.', domain, company, module)),
                AliasLevel.NONE);
        String namespace = "";
        switch (aliasLevel) {
        case FULL:
            namespace = clazz;
            break;
        case MODULE:
            namespace = Strings.join('/', module, clazz);
            break;
        default:
            namespace = Strings.join('/', domain, company, module, clazz);
            break;
        }
        namespace = namespace.replace("Router", "");
        namespace = namespace.replace("Index", "");
        namespace = namespace.replace('_', '/');
        namespace = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN,
                namespace);
        namespace = namespace.replaceAll("/-", "/");
        namespace = Strings.ensureStartsWith(namespace, "/");
        namespace = Strings.ensureEndsWith(namespace, "/");
        return namespace;
    }

    /**
     * Given a list of arguments (as defined by the spec for declaring
     * {@link HttpCallable} objects), generate a string that can use to create
     * the appropriate Spark routes.
     * 
     * @param args a list of arguments
     * @return the path to {@link HttpCallable#setPath(String) assign}
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
     * The namespace is prepended to the relative paths for every
     * {@link Endpoint}.
     */
    protected final String namespace = getCanonicalNamespace(this.getClass()
            .getName());

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected HttpPlugin(ConcourseRuntime concourse) {
        super(concourse);
    }

    /**
     * Return an {@link Iterable iterable} collection of all the
     * {@link HttpCallable endpoints} that are defined in this plugin.
     * 
     * @return all the defined endpoints
     */
    public final Iterable<HttpCallable> endpoints() {
        return new Iterable<HttpCallable>() {

            private final Field[] fields = HttpPlugin.this.getClass().getDeclaredFields();
            private int i = 0;

            @Override
            public Iterator<HttpCallable> iterator() {
                return new AdHocIterator<HttpCallable>() {

                    @Override
                    protected HttpCallable findNext() {
                        if(i >= fields.length) {
                            return null;
                        }
                        else {
                            Field field = fields[i];
                            String name = field.getName();
                            ++i;
                            if(HttpCallable.class.isAssignableFrom(field
                                    .getType())
                                    && (name.startsWith("get")
                                            || name.startsWith("post")
                                            || name.startsWith("put")
                                            || name.startsWith("delete") || name
                                                .startsWith("upsert"))) {
                                List<String> args = Strings
                                        .splitCamelCase(field.getName());
                                String action = args.remove(0);
                                String path = Strings.joinSimple(namespace,
                                        buildSparkPath(args));
                                HttpCallable callable = Reflection.getCasted(
                                        field, HttpPlugin.this);
                                callable.setAction(action);
                                callable.setPath(path);
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

}
