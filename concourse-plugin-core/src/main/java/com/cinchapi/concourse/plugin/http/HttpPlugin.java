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
import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.concourse.plugin.ConcourseRuntime;
import com.cinchapi.concourse.plugin.Plugin;
import com.cinchapi.concourse.util.Reflection;
import com.cinchapi.concourse.util.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

/**
 * An {@link HttpPlugin} is one that exposes functionality via HTTP
 * {@link Endpoint endpoints}.
 * 
 * <p>
 * The name of the HttpPlugin is used for determining the absolute path to
 * prepend to the relative paths defined for each {@link Endpoint}. The name of
 * the class is converted from upper camelcase to lowercase where each word
 * boundary is separated by a forward slash (/) and the words "Router" and
 * "Index" are stripped.
 * </p>
 * <p>
 * For example, a class named {@code com.company.moduleHelloWorldRouter} will
 * have each of its {@link Endpoint endpoints} prepended with
 * {@code /com/company/module/hello/world/}.
 * <p>
 * <p>
 * {@link Endpoint Endpoints} are defined in an HttpPlugin using instance
 * variables. The name of the variable is used to determine the relative path of
 * the endpoint. For example, an Endpoint instance variable named
 * {@code get$Arg1Foo$Arg2} corresponds to the path {@code GET /:arg1/foo/:arg2}
 * relative to the path defined by the HttpPlugin. Each endpoint must respond to
 * one of the HTTP verbs (GET, POST, PUT, DELETE) and serve some payload.
 * <p>
 * You may define multiple endpoints that process the same path as long as each
 * one responds to a different HTTP verb (i.e. you may have GET /path/to/foo and
 * POST /path/to/foo).
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class HttpPlugin extends Plugin {

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
        String clazz = toks[toks.length - 1];
        String namespace;
        if(name.startsWith("com.cinchapi.concourse.server")){
            namespace = clazz;
        }
        else {
            namespace = name;
            namespace = namespace.replaceFirst("com.cinchapi.concourse.plugin", "");
            namespace = namespace.replaceFirst("com.cinchapi.concourse", "");
            namespace = namespace.replaceFirst("com.cinchapi", "");
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
     * {@link Endpoint endpoints} that are defined in this plugin.
     * 
     * @return all the defined endpoints
     */
    public final Iterable<Endpoint> endpoints() {
        return new Iterable<Endpoint>() {

            private final Field[] fields = HttpPlugin.this.getClass()
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
                            if(Endpoint.class.isAssignableFrom(field.getType())
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
                                Endpoint callable = Reflection.getCasted(field,
                                        HttpPlugin.this);
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
