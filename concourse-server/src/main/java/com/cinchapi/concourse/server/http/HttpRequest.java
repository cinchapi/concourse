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

import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.JsonElement;

/**
 * Defines all the actions that can be taken on a request to an
 * {@link EndpointContainer}.
 * 
 * @author Jeff Nelson
 */
public interface HttpRequest {

    /**
     * Gets the value of the provided attribute
     * 
     * @param attribute The attribute value or null if not present
     */
    public Object attribute(String attribute);

    /**
     * Sets an attribute on the request (can be fetched in filters/routes later
     * in the chain)
     * 
     * @param attribute The attribute
     * @param value The attribute value
     */
    public void attribute(String attribute, Object value);

    /**
     * Returns all attributes
     */
    public Set<String> attributes();

    /**
     * Returns the request body sent by the client
     */
    public String body();

    /**
     * Parse the request body into a {@link JsonElement}.
     * 
     * @return
     */
    public JsonElement bodyAsJson();

    /**
     * Returns the length of request.body
     */
    public int contentLength();

    /**
     * Returns the content type of the body
     */
    public String contentType();

    /**
     * Returns the context path
     */
    public String contextPath();

    /**
     * Gets cookie by name.
     * 
     * @param name name of the cookie
     * @return cookie value or null if the cookie was not found
     */
    public String cookie(String name);

    /**
     * @return request cookies (or empty Map if cookies dosn't present)
     */
    public Map<String, String> cookies();

    /**
     * Return a parameter associated with the request being processed.
     * <p>
     * Prepend the name of the parameter with {@code ":"} if it is a variable in
     * the route (i.e. /foo/:id). Otherwise, if the name of the parameter does
     * not start with {@code ":"} then it is assumed to be a variable in the
     * query string. (i.e. /foo?id=).
     * </p>
     * 
     * @param param
     * @return the value associated with the param or {@code null} if it is not
     *         provided or not found
     */
    public String getParamValue(String param);

    /**
     * Given a list of param aliases, return the first existing value that is
     * encountered.
     * <p>
     * Prepend the name of the parameter with {@code ":"} if it is a variable in
     * the route (i.e. /foo/:id). Otherwise, if the name of the parameter does
     * not start with {@code ":"} then it is assumed to be a variable in the
     * query string. (i.e. /foo?id=).
     * </p>
     * 
     * @param aliases
     * @return the first found value associated with one of the aliases or
     *         {@code null} if none of the aliases are provided with a value.
     */
    public String getParamValueOrAlias(String... aliases);

    /**
     * Return the list of values mapped from a parameter associated with the
     * request being processed. This method is only appropriate for query
     * params and will not work for route variables (because those cannot
     * contain multiple values).
     * <p>
     * <strong>NOTE:</strong> If there are no values for {@code param}, then an
     * empty list is returned.
     * </p>
     * 
     * @param param
     * @return the (possibly empty) list of values
     */
    public List<String> getParamValues(String param);

    /**
     * Returns all headers
     */
    public Set<String> headers();

    /**
     * Returns the value of the provided header
     */
    public String headers(String header);

    /**
     * Returns the host
     */
    public String host();

    /**
     * Returns the client's IP address
     */
    public String ip();

    /**
     * Returns the value of the provided route pattern parameter.
     * Example: parameter 'name' from the following pattern: (get
     * '/hello/:name')
     * 
     * @return null if the given param is null or not found
     */
    public String params(String param);

    /**
     * Returns the path info
     * Example return: "/example/foo"
     */
    public String pathInfo();

    /**
     * Returns the server port
     */
    public int port();

    /**
     * Returns all query parameters
     */
    public Set<String> queryParams();

    /**
     * Returns the value of the provided queryParam
     * Example: query parameter 'id' from the following request URI:
     * /hello?id=foo
     */
    public String queryParams(String queryParam);

    /**
     * Returns the query string
     */
    public String queryString();

    /**
     * Returns request method e.g. GET, POST, PUT, ...
     */
    public String requestMethod();

    /**
     * Returns the scheme
     */
    public String scheme();

    /**
     * Returns the servlet path
     */
    public String servletPath();

    /**
     * Returns an arrat containing the splat (wildcard) parameters
     */
    public String[] splat();

    /**
     * Returns the URL string
     */
    public String url();

    /**
     * Returns the user-agent
     */
    public String userAgent();

}
