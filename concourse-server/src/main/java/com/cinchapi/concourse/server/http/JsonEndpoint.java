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

import com.cinchapi.concourse.plugin.http.Endpoint;
import com.cinchapi.concourse.plugin.http.HttpRequest;
import com.cinchapi.concourse.plugin.http.HttpResponse;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.google.gson.JsonElement;

/**
 * An {@link Endpoint} that serves a {@link JsonElement} as its response.
 * 
 * @author Jeff Nelson
 */
public abstract class JsonEndpoint extends Endpoint {

    @Override
    public final String serve(HttpRequest request, HttpResponse response,
            AccessToken creds, TransactionToken transaction, String environment)
            throws Exception {
        return serve(request, creds, transaction, environment, response)
                .toString();
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
     * @return the {@link JsonElement payload}
     * @throws Exception
     */
    protected abstract JsonElement serve(HttpRequest request,
            AccessToken creds, TransactionToken transaction,
            String environment, HttpResponse response) throws Exception;

}
