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

import spark.HaltException;
import spark.Response;
import spark.Route;

import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A {@link Request} is processed by a Route in order to return a JSON
 * payload. In particular, this class takes care of some scaffolding,
 * error handling, etc.
 * <p>
 * Each {@link Request} will return a JSON response with the following members:
 * <ul>
 * <li>status - success or failed</li>
 * <li>payload - the relevant data returned from the request or an error message
 * </li>
 * </ul>
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class Request extends Route implements Rewritable {

    /**
     * Check to ensure that any of the specified required {@link params} is
     * not {@code null} or an empty string. If so, halt the request
     * immediately.
     * 
     * @param params
     */
    protected static final void require(Object... params) {
        for (Object param : params) {
            if(param == null
                    || (param instanceof String && Strings
                            .isNullOrEmpty((String) param))) {
                halt(400, "Request is missing a required parameter");
            }
        }
    }

    /**
     * A message indicating the request succeeded.
     */
    private static String STATUS_SUCCESS = "success";

    /**
     * A message indicating that the request failed.
     */
    private static String STATUS_FAILED = "failed";

    /**
     * The associated request.
     */
    private spark.Request request;

    /**
     * The associated response.
     */
    private Response response;

    private String uri;

    /**
     * Construct a new instance.
     * 
     * @param path
     */
    protected Request(String path) {
        super(path);
        this.uri = path;
    }

    public final String getUri() {
        return uri;
    }

    @Override
    public final Object handle(spark.Request request, spark.Response response) {
        this.request = request;
        this.response = response;
        JsonObject json = new JsonObject();
        try {
            json.addProperty("status", STATUS_SUCCESS);
            json.add("payload", handle());
        }
        catch (HaltException e) {
            throw e;
        }
        catch (Exception e) {

            json.addProperty("status", STATUS_FAILED);
            json.addProperty("payload", e.getMessage());
        }
        this.response.type("application/json");
        return json;
    }

    /**
     * Return a parameter associated with the request being processed.
     * <p>
     * Prepend the name of the parameter with ":" if it is a variable in the
     * route (i.e. /foo/:id). Otherwise, it is assumed to be a query param (i.e.
     * /foo?id=).
     * </p>
     * 
     * @param param
     * @return the value associated with the param
     */
    protected String get(String param) {
        return param.startsWith(":") ? request.params(param) : request
                .queryParams(param);
    }

    /**
     * TODO
     * 
     * @param param
     * @return
     */
    protected String[] getAll(String param) {
        try {
            return request.queryMap(param).values();
        }
        catch (NullPointerException e) { // the param is not in the map, so
                                         // return an empty array
            return new String[] {};
        }
    }

    /**
     * Do the work to handle the request and return a {@link JsonElement}
     * payload.
     * 
     * @return the payload
     */
    protected abstract JsonElement handle();

}
