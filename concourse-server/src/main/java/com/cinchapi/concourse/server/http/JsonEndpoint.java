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

import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.DataServices;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * An {@link Endpoint} that can
 * {@link #serve(HttpRequest, AccessToken, TransactionToken, String, HttpResponse)}
 * a request with a generic object, that is intelligently serialized to an
 * appropriate JSON payload.
 * 
 * @author Jeff Nelson
 */
public abstract class JsonEndpoint extends Endpoint {

    /**
     * The encoder to use to serialize the JSON response.
     */
    private final Gson encoder;

    /**
     * Construct a new instance that uses that default {@link Gson json encoder}
     */
    public JsonEndpoint() {
        this(DataServices.gson());
    }

    /**
     * Construct a new instance that uses the specified {@link Gson json
     * encoder}.
     * 
     * @param encoder an instance of {@link Gson} to use when encoding the
     *            response from
     *            {@link #serve(HttpRequest, AccessToken, TransactionToken, String, HttpResponse)}
     * 
     */
    public JsonEndpoint(Gson encoder) {
        super();
        this.encoder = encoder;
    }

    /**
     * Construct a new instance.
     * 
     * @param encoder an instance of {@link Gson} to use when encoding the
     *            response from
     *            {@link #serve(HttpRequest, AccessToken, TransactionToken, String, HttpResponse)}
     * @param action
     * @param path
     */
    public JsonEndpoint(Gson encoder, String action, String path) {
        super(action, path);
        this.encoder = encoder;
    }

    /**
     * Construct a new instance.
     * 
     * @param action
     * @param path
     */
    public JsonEndpoint(String action, String path) {
        this(DataServices.gson(), action, path);
    }

    @Override
    public final String serve(HttpRequest request, HttpResponse response,
            AccessToken creds, TransactionToken transaction, String environment)
            throws Exception {
        Object payload = serve(request, creds, transaction, environment,
                response);
        JsonElement encoded;
        if(payload == null) {
            encoded = NO_DATA;
        }
        else if(payload instanceof JsonElement) {
            encoded = (JsonElement) payload;
        }
        else {
            encoded = encoder.toJsonTree(payload);
        }
        return encoded.toString();
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
     * @param creds an {@link AccessToken} for the authenticated user, if a user
     *            session exists
     * @param transaction a {@link TransactionToken} for appropriately routing
     *            the data actions
     * @param environment the environment of the
     *            {@link com.cinchapi.concourse.plugin.ConcourseRuntime} in
     *            which the data action should occur
     * @param response an {@link HttpResponse object} that contains all the
     *            information that should be issued within the response
     * @return the payload
     * @throws Exception
     */
    protected abstract Object serve(HttpRequest request, AccessToken creds,
            TransactionToken transaction, String environment,
            HttpResponse response) throws Exception;

}
