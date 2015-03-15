/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.server.http;

import org.cinchapi.concourse.thrift.TSecurityException;
import org.cinchapi.vendor.spark.HaltException;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

/**
 * An {@link Endpoint} is processed by a {@link Router} in order to return a
 * JSON
 * payload. In particular, this class takes care of some scaffolding,
 * error handling, etc.
 * <p>
 * Each {@link Endpoint} will return a JSON response with the following members:
 * <ul>
 * <li>status - success or failed</li>
 * <li>payload - the relevant data returned from the request or an error message
 * </li>
 * </ul>
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class Endpoint extends BaseRewritableRoute {

    /**
     * A {@link JsonElement} that represents the lack of any data being
     * returned.
     */
    protected static JsonNull NO_DATA = JsonNull.INSTANCE;

    /**
     * Construct a new instance.
     * 
     * @param path
     */
    public Endpoint(String path) {
        super(path);
    }

    @Override
    public final Object handle() {
        this.response.type("application/json");
        try {
            return serve();
        }
        catch (HaltException e) {
            throw e;
        }
        catch (Exception e) {
            if(e instanceof TSecurityException
                    || e instanceof SecurityException) {
                response.status(401);
                //TODO remove auth token cookie
            }
            else if(e instanceof IllegalArgumentException){
                response.status(400);
            }
            else{
                response.status(500);
            }
            JsonObject json = new JsonObject();
            json.addProperty("error", e.getMessage());
            return json;
        }
    }

    /**
     * Serve the request with a {@link JsonElement} payload.
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
    protected abstract JsonElement serve() throws Exception;

}
