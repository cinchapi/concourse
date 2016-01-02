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

import spark.HaltException;
import spark.Request;
import spark.Response;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.errors.HttpError;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.Logger;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A {@link Resource} is an {@link Endpoint} returns a JSON payload. This base
 * class takes care of some scaffolding, error handling, etc.
 * 
 * @author Jeff Nelson
 */
public abstract class Resource extends Endpoint {

    /**
     * A {@link JsonElement} that represents the lack of any data being
     * returned.
     */
    protected static JsonObject NO_DATA = new JsonObject();

    @Override
    public final Object handle(Request request, Response response,
            AccessToken creds, TransactionToken transaction, String environment) {
        response.type("application/json");
        try {
            return serve(request, response, creds, transaction, environment);
        }
        catch (HaltException e) {
            throw e;
        }
        catch (Exception e) {
            if(e instanceof HttpError) {
                response.status(((HttpError) e).getCode());
            }
            else if(e instanceof SecurityException
                    || e instanceof java.lang.SecurityException) {
                response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
                response.status(401);
            }
            else if(e instanceof IllegalArgumentException) {
                response.status(400);
            }
            else {
                response.status(500);
                Logger.error("", e);
            }
            JsonObject json = new JsonObject();
            json.addProperty("error", e.getMessage());
            return json;
        }
    }

    /**
     * Serve the {@code request} and return a {@link JsonElement} payload.
     * <p>
     * If this method returns, then the Router will assume that the request was
     * successful. If, for any reason, an error occurs, this method should throw
     * an Exception and the Router will wrap that in the appropriate response to
     * the caller.
     * </p>
     * 
     * @return the payload
     * @throws Exception
     */
    protected abstract JsonElement serve(Request request, Response response,
            AccessToken creds, TransactionToken transaction, String environment)
            throws Exception;

}
