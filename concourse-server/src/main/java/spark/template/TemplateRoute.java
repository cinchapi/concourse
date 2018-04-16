/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package spark.template;

import java.util.Collections;
import java.util.Map;

import spark.Route;

/**
 * Abstract template route.
 * 
 * Example:
 * 
 * <pre>
 * {@code
 * Spark.get(new TemplateRoute("/hello/:name") {
 *    public Object handle(Request request, Response response) {
 *       Person person = Person.find(request.params("name"));
 *       return template("hello").render("person", person);
 *    }
 * });}
 * </pre>
 */
public abstract class TemplateRoute extends Route {

    /**
     * Constructor
     * 
     * @param path The route path which is used for matching. (e.g. /hello,
     *            users/:name)
     */
    protected TemplateRoute(String path) {
        super(path);
    }

    /**
     * Create and return a template with the specified name.
     * 
     * @param name The template name
     * 
     * @return The template with the specified name
     */
    public abstract Template template(String name);

    /**
     * Abstract template.
     */
    protected abstract class Template {
        private final Map<String, Object> emptyContext = Collections.emptyMap();

        protected Template() {
            // empty
        }

        /**
         * Render this template with the specified context.
         * 
         * @param context The context with which to render this template
         * 
         * @return The content of this template rendered with the specified
         *         context
         */
        public abstract Object render(Map<String, Object> context);

        /**
         * Render this template with an empty context.
         * 
         * @return The content of this template rendered with an empty context
         */
        public final Object render() {
            return render(emptyContext);
        }
    }
}