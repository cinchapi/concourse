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
package com.cinchapi.concourse.server.http;

import com.cinchapi.concourse.plugin.http.HttpCallable;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.ObjectUtils;
import com.cinchapi.concourse.util.Reflection;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;

import spark.Request;
import spark.Response;
import spark.template.MustacheTemplateRoute;

/**
 * An {@link Endpoint} defines logic to handle an HTTP request that is made to a
 * certain path.
 * <p>
 * This class provides some utility functions around some of the native route
 * components with a cleaner interface.
 * </p>
 * <h2>Preconditions</h2>
 * <ul>
 * <li>Use the {@link #require(Object...)} method to ensure that the necessary
 * variables are all non-empty before continuing in the route, halting if the
 * check fails.</li>
 * </ul>
 * <h2>Redirection</h2>
 * <ul>
 * <li>Use the {@link Response#redirect(String) response.redirect(String)}
 * method to trigger a browser redirect to another location</li>
 * </ul>
 * 
 * @author Jeff Nelson
 */
// NOTE: We are extending the MustacheTemplateRoute this high up in the chain so
// that View subclasses can access the necessary methods while also benefiting
// from some of the non-view scaffolding that happens in this and other bases
// classes.
public abstract class Endpoint extends MustacheTemplateRoute implements
        HttpCallable {

    /**
     * Check to ensure that none of the specified {@link params} is {@code null}
     * or an empty string or an empty collection. If so, halt
     * the request immediately.
     * 
     * @param params
     */
    protected static final void require(Object... params) {
        for (Object param : params) {
            if(ObjectUtils.isNullOrEmpty(param)) {
                halt(400, "Request is missing a required parameter");
            }
        }
    }

    /**
     * A flag that tracks whether path for this Endpoint has been
     * {@link #setPath(String) set}.
     */
    private boolean hasPath = false;

    /**
     * The HTTP verb that is served by this Endpoint.
     */
    private String action;

    /**
     * Construct a new instance.
     * 
     * @param relativePath
     */
    protected Endpoint() {
        super(""); // The path is set by the Router using the #setPath method
    }

    @Override
    public final Object handle(Request request, Response response) {
        // The HttpRequests preprocessor assigns attributes to the request in
        // order for the Endpoint to make calls into ConcourseServer.
        AccessToken creds = (AccessToken) request
                .attribute(GlobalState.HTTP_ACCESS_TOKEN_ATTRIBUTE);
        String environment = MoreObjects.firstNonNull((String) request
                .attribute(GlobalState.HTTP_ENVIRONMENT_ATTRIBUTE),
                GlobalState.DEFAULT_ENVIRONMENT);
        String fingerprint = (String) request
                .attribute(GlobalState.HTTP_FINGERPRINT_ATTRIBUTE);

        // Check basic authentication: is an AccessToken present and does the
        // fingerprint match?
        if((boolean) request.attribute(GlobalState.HTTP_REQUIRE_AUTH_ATTRIBUTE)
                && creds == null) {
            halt(401);
        }
        if(!Strings.isNullOrEmpty(fingerprint)
                && !fingerprint.equals(HttpRequests.getFingerprint(request))) {
            Logger.warn(
                    "Request made with mismatching fingerprint. Expecting {} but got {}",
                    HttpRequests.getFingerprint(request), fingerprint);
            halt(401);
        }
        TransactionToken transaction = null;
        try {
            Long timestamp = Longs.tryParse((String) request
                    .attribute(GlobalState.HTTP_TRANSACTION_TOKEN_ATTRIBUTE));
            transaction = creds != null && timestamp != null ? new TransactionToken(
                    creds, timestamp) : transaction;
        }
        catch (NullPointerException e) {}
        return handle(request, response, creds, transaction, environment);
    }

    /**
     * Handle the request that has been made to the path that corresponds to
     * this {@link Endpoint}.
     * 
     * @param request
     * @param response
     * @param creds
     * @param transaction
     * @param environment
     * @return the content to be set in the response
     */
    protected abstract Object handle(Request request, Response response,
            AccessToken creds, TransactionToken transaction, String environment);

    /**
     * Return the path for this Endpoint.
     * 
     * @return the path
     */
    // NOTE: This method is called "path" instead of "getPath" because a parent
    // class already has a package private class named "getPath"
    protected String path() {
        return Reflection.get("path", this);
    }

    @Override
    public void setPath(String path) {
        Preconditions.checkState(!hasPath,
                "The path for the endpoint has already been set");
        path = (path.startsWith("/") ? path : "/" + path).toLowerCase();
        Reflection.set("path", path, this);
        hasPath = true;
    }

    @Override
    public void setAction(String action) {
        this.action = action;
    }
    
    @Override
    public String getAction(){
        return action;
    }
}
