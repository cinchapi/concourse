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
package org.cinchapi.concourse.server.http.routers;

import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.http.Endpoint;
import org.cinchapi.concourse.server.http.HttpRequests;
import org.cinchapi.concourse.server.http.Router;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.DataServices;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * The core/default router.
 * 
 * @author Jeff Nelson
 */
public class IndexRouter extends Router {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public IndexRouter(ConcourseServer concourse) {
        super(concourse);
    }

    @Override
    public void routes() {

        /*
         * ########################
         * #### AUTHENTICATION ####
         * ########################
         */

        /**
         * POST /login
         */
        post(new Endpoint("/login") {

            @Override
            protected JsonElement serve() throws Exception {
                JsonElement body = this.request.bodyAsJson();
                if(body.isJsonObject()) {
                    JsonObject creds = (JsonObject) body;
                    ByteBuffer username = ByteBuffers.fromString(creds.get(
                            "username").getAsString());
                    ByteBuffer password = ByteBuffers.fromString(creds.get(
                            "password").getAsString());
                    AccessToken access = concourse.login(username, password,
                            environment);
                    String token = HttpRequests.encodeAuthToken(access,
                            environment);
                    this.response.cookie("/",
                            GlobalState.HTTP_AUTH_TOKEN_COOKIE, token, 900,
                            false);
                    JsonObject response = new JsonObject();
                    response.add("token", new JsonPrimitive(token));
                    response.add("environment", new JsonPrimitive(environment));

                    return response;
                }
                else {
                    throw new IllegalArgumentException(
                            "Please specify username/password credentials "
                                    + "in a JSON object");
                }
            }

        });

        /**
         * GET /
         */
        get(new Endpoint("/") {

            @Override
            protected JsonElement serve() throws Exception {
                JsonObject payload = new JsonObject();
                payload.addProperty("environment", environment);
                return payload;
            }

        });

        get(new Endpoint("/:arg1") {

            @Override
            protected JsonElement serve() throws Exception {
                // TODO what about transaction
                String arg1 = getParamValue(":arg1");
                String ts = getParamValue("timestamp");
                Long timestamp = ts == null ? null : Timestamp.parse(ts)
                        .getMicros();
                Object data;
                if(StringUtils.isNumeric(arg1)) {
                    long record = Long.parseLong(arg1);
                    data = timestamp == null ? concourse.selectRecord(record,
                            accessToken, null, environment) : concourse
                            .selectRecordTime(record, timestamp, accessToken,
                                    null, environment);
                }
                else {
                    data = timestamp == null ? concourse.browseKey(arg1,
                            accessToken, null, environment) : concourse
                            .browseKeyTime(arg1, timestamp, accessToken,
                                    null, environment);
                }          
                return DataServices.gson().toJsonTree(data);
            }

        });

    }
}
