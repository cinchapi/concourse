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

import javax.annotation.Nullable;

import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.gson.JsonObject;

/**
 * An {@link Endpoint} is the most basic construct in the HTTP framework. An
 * Endpoint is uniquely identifiable and accessible from a <em<route</em> that
 * is made up of an HTTP verb/action and a URI.
 * <p>
 * An Endpoint's route can be provided upon
 * {@link Endpoint#Endpoint(String, String) construction} or it can be inferred
 * from the name of the instance variable to which the Endpoint is bound in an
 * {@link EndpointContainer}.
 * </p>
 * <p>
 * When declaring Endpoints anonymously within an {@link EndpointContainer}, be
 * sure to use variable names that conform to the roting spec.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class Endpoint {

    /**
     * Create an {@link Endpoint} that is bound to a variable whose name is used
     * to assign the {@link #getAction() action} and {@link #getPath() path}.
     */
    public Endpoint() {
        this(null, null);
    }

    /**
     * Create an {@link Endpoint} that uses the provided {@code action} and
     * {@code path}.
     * 
     * @param action the HTTP verb to which this {@link Endpoint} responds
     * @param path the relative path of the {@link Endpoint}
     */
    public Endpoint(String action, String path) {
        this.action = action;
        this.path = path;
    }

    /**
     * A {@link JsonElement} that represents the lack of any data being
     * returned.
     */
    protected static JsonObject NO_DATA = new JsonObject();

    /**
     * The HTTP verb to which this endpoint responds.
     */
    private String action;

    /**
     * The full URI at which this endpoint is reachable.
     */
    private String path;

    /**
     * Return the HTTP verb/action to which this endpoint responds.
     * <p>
     * This value is configured using {@link #setAction(String)}.
     * </p>
     * 
     * @return the action for this Endpoint
     */
    public String getAction() {
        return action;
    }

    /**
     * Return the content type that this {@link Endpoint} returns from the
     * {@link #serve(HttpRequest, HttpResponse, AccessToken, TransactionToken, String)}
     * method.
     * 
     * @return the content type
     */
    public ContentType getContentType() {
        return ContentType.JSON;
    }

    /**
     * Return the path at which this endpoint is reachable.
     * 
     * @return the path for this Endpoint
     */
    public String getPath() {
        return path;
    }

    /**
     * Serve the {@code request} and return the appropriate payload with the
     * {@code response}.
     * 
     * <p>
     * If this method returns, it is assumed that the request was successful.
     * If, for any reason, an error occurs, this method should throw an
     * Exception and it will be wrapped in the appropriate response for the
     * caller.
     * </p>
     * 
     * @param request an {@link HttpRequest object} that contains all the
     *            information about the request
     * @param response an {@link HttpResponse object} that contains all the
     *            information that should be issued within the response
     * @param creds an {@link AccessToken} for the authenticated user, if a user
     *            session exists
     * @param transaction a {@link TransactionToken} for appropriately routing
     *            the data actions
     * @param environment the environment of the {@link ConcourseRuntime} in
     *            which the data action should occur
     * @return the payload
     * @throws Exception
     */
    public abstract String serve(HttpRequest request, HttpResponse response,
            @Nullable AccessToken creds,
            @Nullable TransactionToken transaction, String environment)
            throws Exception;

    /**
     * Items that can be returned from the {@link Endpoint#getContentType()}
     * method.
     * 
     * @author Jeff Nelson
     */
    public enum ContentType {
        JSON;

        @Override
        public String toString() {
            switch (this) {
            case JSON:
                return "application/json";
            default:
                return "text/html";
            }
        }
    }
}
