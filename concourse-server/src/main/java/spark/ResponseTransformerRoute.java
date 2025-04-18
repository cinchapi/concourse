/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package spark;

/**
 * A ResponseTransformerRoute is built up by a path (for url-matching) and the
 * implementation of the 'render' method. ResponseTransformerRoute instead of
 * returning the result of calling toString() as body, it returns the result of
 * calling render method.
 * 
 * The primary purpose of this kind of Route is provide a way to create generic
 * and reusable transformers. For example to convert an Object to JSON format.
 * 
 * @author alex
 */
public abstract class ResponseTransformerRoute extends Route {

    protected ResponseTransformerRoute(String path) {
        super(path);
    }

    protected ResponseTransformerRoute(String path, String acceptType) {
        super(path, acceptType);
    }

    /**
     * Method called for rendering the output.
     * 
     * @param model
     *            object used to render output.
     * 
     * @return message that it is sent to client.
     */
    public abstract String render(Object model);

}
