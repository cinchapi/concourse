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

import java.lang.reflect.Field;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import spark.Request;
import spark.Response;
import spark.template.MustacheTemplateRoute;

/**
 * A {@link RewritableRoute} is one that can have its path altered in certain
 * circumstances. This base class provides some methods to do re-writing in a
 * convenient way.
 * <p>
 * <em>Rather than extending this class directly, consider using
 * {@link BaseRewritableRoute} as a parent class since it takes care of some
 * boilerplate scaffolding and provides some helpful utility functions.</em>
 * </p>
 * 
 * @author Jeff Nelson
 */
// NOTE: We are extending the MustacheTemplateRoute this high up in the chain so
// that View subclasses can access the necessary methods while also benefiting
// from some of the non-view scaffolding that happens in this and other bases
// classes.
public abstract class RewritableRoute extends MustacheTemplateRoute {

    /**
     * The {@link Request} object that is associated with this {@link RewritableRoute}.
     * While this object is accessible to subclasses, caution should be
     * exercised when operating on this object directly.
     */
    protected Request request;

    /**
     * The {@link Response} object that is associated with this {@link RewritableRoute}.
     * While this object is accessible to subclasses, caution should be
     * exercised when operating on this object directly.
     */
    protected Response response;

    /**
     * Construct a new instance.
     * 
     * @param relativePath
     */
    protected RewritableRoute(String relativePath) {
        super(relativePath);
    }

    /**
     * Get the path that describes this route.
     * 
     * @return the path
     */
    protected String getRoutePath() {
        try {
            Class<?> parent = this.getClass().getSuperclass();
            Field field = null;
            while (field == null && parent != Object.class) {
                try {
                    field = parent.getDeclaredField("path");
                }
                catch (NoSuchFieldException e) {
                    parent = parent.getSuperclass();
                }
            }
            Preconditions.checkState(field != null);
            field.setAccessible(true);
            return (String) field.get(this);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Rewrite this {@link RewritableRoute} by prepending the {@code namespace} to the
     * relative path that was specified in the constructor.
     * 
     * @param namespace
     */
    protected void prepend(String namespace) {
        if(!Strings.isNullOrEmpty(namespace)
                && !namespace.equalsIgnoreCase("index")) {
            namespace = namespace.toLowerCase();
            try {
                Class<?> parent = this.getClass().getSuperclass();
                Field field = null;
                while (field == null && parent != Object.class) {
                    try {
                        field = parent.getDeclaredField("path");
                    }
                    catch (NoSuchFieldException e) {
                        parent = parent.getSuperclass();
                    }
                }
                Preconditions.checkState(field != null);
                field.setAccessible(true);
                String path = (String) field.get(this);
                path = path.startsWith("/") ? path : "/" + path;
                path = namespace + path;
                field.set(this, path);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

    }
}

