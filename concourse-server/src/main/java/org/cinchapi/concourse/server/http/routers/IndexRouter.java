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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.http.Endpoint;
import org.cinchapi.concourse.server.http.HttpRequests;
import org.cinchapi.concourse.server.http.Router;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.DataServices;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
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
     * Given two arguments, figure out which is the key and which is the record.
     * This method returns an array where the first element is the key and the
     * second is the record.
     * 
     * @param arg1
     * @param arg2
     * @return an array with the key followed by the record
     */
    private static Object[] pickKeyAndRecord(String arg1, String arg2) {
        Long record = Longs.tryParse(arg1);
        String key;
        if(record != null) {
            key = arg2;
        }
        else {
            key = arg1;
            record = Long.parseLong(arg2);
        }
        return new Object[] { key, record };
    }

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
         * POST /logout
         */
        post(new Endpoint("/logout") {

            @Override
            protected JsonElement serve() throws Exception {
                concourse.logout(creds, environment);
                response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
                return NO_DATA;
            }

        });

        /*
         * ######################
         * #### TRANSACTIONS ####
         * ######################
         */

        /**
         * GET /stage
         */
        get(new Endpoint("/stage") {

            @Override
            protected JsonElement serve() throws Exception {
                TransactionToken transaction = concourse.stage(creds,
                        environment);
                String token = Long.toString(transaction.timestamp);
                this.response.cookie("/",
                        GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE, token, 900,
                        false);
                JsonObject data = new JsonObject();
                data.addProperty("transaction", token);
                return data;
            }

        });

        /**
         * GET /commit
         */
        get(new Endpoint("/commit") {

            @Override
            protected JsonElement serve() throws Exception {
                boolean result = concourse.commit(creds, transaction,
                        environment);
                this.response
                        .removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
                return new JsonPrimitive(result);
            }

        });

        /**
         * GET /abort
         */
        get(new Endpoint("/abort") {

            @Override
            protected JsonElement serve() throws Exception {
                concourse.abort(creds, transaction, environment);
                this.response
                        .removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
                return NO_DATA;
            }

        });

        /**
         * GET /
         */
        get(new Endpoint("/") {

            @Override
            protected JsonElement serve() throws Exception {
                String ccl = getParamValue("query");
                Object data;
                if(!Strings.isNullOrEmpty(ccl)) {
                    List<String> keys = getParamValues("select");
                    String ts = getParamValue("timestamp");
                    Long timestamp = ts != null ? Longs.tryParse(ts) : null;
                    if(keys.isEmpty()) {
                        data = timestamp == null ? concourse.selectCcl(ccl,
                                creds, transaction, environment) : concourse
                                .selectCclTime(ccl, timestamp, creds,
                                        transaction, environment);
                    }
                    else {
                        data = timestamp == null ? concourse.selectKeysCcl(
                                keys, ccl, creds, transaction, environment)
                                : concourse.selectKeysCclTime(keys, ccl,
                                        timestamp, creds, transaction,
                                        environment);
                    }
                }
                else {
                    data = concourse.find(creds, null, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }

        });

        /**
         * POST /
         * PUT /
         */
        upsert(new Endpoint("/") {

            @Override
            protected JsonElement serve() throws Exception {
                String json = request.body();
                Set<Long> records = concourse.insertJson(json, creds,
                        transaction, environment);
                return DataServices.gson().toJsonTree(records);
            }

        });

        /**
         * GET /record[?timestamp=<ts>]
         * GET /key[?timestamp=<ts>]
         */
        get(new Endpoint("/:arg1") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String ts = getParamValue("timestamp");
                Long timestamp = ts == null ? null : Timestamp.parse(ts)
                        .getMicros();
                Long record = Longs.tryParse(arg1);
                Object data;
                if(record != null) {
                    data = timestamp == null ? concourse.selectRecord(record,
                            creds, null, environment) : concourse
                            .selectRecordTime(record, timestamp, creds,
                                    transaction, environment);
                }
                else {
                    data = timestamp == null ? concourse.browseKey(arg1, creds,
                            null, environment) : concourse.browseKeyTime(arg1,
                            timestamp, creds, transaction, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }

        });

        /**
         * POST|PUT /record
         * POST|PUT /key
         */
        upsert(new Endpoint("/:arg1") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                Long record = Longs.tryParse(arg1);
                Object result;
                if(record != null) {
                    String json = request.body();
                    result = concourse.insertJsonRecord(json, record, creds,
                            transaction, environment);
                }
                else {
                    TObject value = Convert.javaToThrift(Convert
                            .stringToJava(request.body()));
                    result = concourse.addKeyValue(arg1, value, creds,
                            transaction, environment);
                }
                return DataServices.gson().toJsonTree(result);
            }

        });

        /**
         * DELETE /record
         */
        delete(new Endpoint("/:record") {

            @Override
            protected JsonElement serve() throws Exception {
                long record = Long.parseLong(getParamValue(":record"));
                concourse.clearRecord(record, creds, transaction, environment);
                return NO_DATA;
            }

        });

        /**
         * GET /key/record[?timestamp=<ts>]
         * GET /record/key[?timestamp=<ts>]
         */
        get(new Endpoint("/:arg1/:arg2") {

            @Override
            protected JsonElement serve() throws Exception {
                String ts = getParamValue("timestamp");
                Long timestamp = ts == null ? null : Timestamp.parse(ts)
                        .getMicros();
                String arg1 = getParamValue(":arg1");
                String arg2 = getParamValue(":arg2");
                Object[] args = pickKeyAndRecord(arg1, arg2);
                String key = (String) args[0];
                Long record = (Long) args[1];
                Object data;
                if(timestamp == null) {
                    data = concourse.selectKeyRecord(key, record, creds,
                            transaction, environment);
                }
                else {
                    data = concourse.selectKeyRecordTime(key, record,
                            timestamp, creds, transaction, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }

        });

        /**
         * POST /record/key
         * POST /key/record
         */
        post(new Endpoint("/:arg1/:arg2") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String arg2 = getParamValue(":arg2");
                Object[] args = pickKeyAndRecord(arg1, arg2);
                String key = (String) args[0];
                Long record = (Long) args[1];
                TObject value = Convert.javaToThrift(Convert
                        .stringToJava(request.body()));
                boolean result = concourse.addKeyValueRecord(key, value,
                        record, creds, transaction, environment);
                return DataServices.gson().toJsonTree(result);
            }

        });

        /**
         * PUT /record/key
         * PUT /key/record
         */
        put(new Endpoint("/:arg1/:arg2") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String arg2 = getParamValue(":arg2");
                Object[] args = pickKeyAndRecord(arg1, arg2);
                String key = (String) args[0];
                Long record = (Long) args[1];
                TObject value = Convert.javaToThrift(Convert
                        .stringToJava(request.body()));
                concourse.setKeyValueRecord(key, value, record, creds,
                        transaction, environment);
                return NO_DATA;
            }

        });

        /**
         * DELETE /record/key
         * DELETE /key/record
         */
        delete(new Endpoint("/:arg1/:arg2") {

            @Override
            protected JsonElement serve() throws Exception {
                Object[] args = pickKeyAndRecord(getParamValue(":arg1"),
                        getParamValue(":arg2"));
                String key = (String) args[0];
                Long record = (Long) args[1];
                String body = request.body();
                if(StringUtils.isBlank(body)) {
                    concourse.clearKeyRecord(key, record, creds, transaction,
                            environment);
                    return NO_DATA;
                }
                else {
                    TObject value = Convert.javaToThrift(Convert
                            .stringToJava(request.body()));
                    Object data = concourse.removeKeyValueRecord(key, value,
                            record, creds, transaction, environment);
                    return DataServices.gson().toJsonTree(data);
                }
            }

        });

        /**
         * GET /record/audit?timestamp=<ts>
         * GET /record/audit?start=<ts>&end=<te>
         */
        get(new Endpoint("/:arg1/audit") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String ts = getParamValue("start");
                String te = getParamValue("end");
                ts = Objects.firstNonNull(ts, getParamValue("timestamp"));
                Long record = Timestamp.parse(arg1).getMicros();
                Object data;
                if(ts == null) {
                    data = concourse.auditRecord(record, creds, null,
                            environment);
                }
                else if(te == null) {
                    data = concourse
                            .auditRecordStart(record, Timestamp.parse(ts).getMicros(),
                                    creds, transaction, environment);
                }
                else {
                    data = concourse.auditRecordStartEnd(record,
                            Timestamp.parse(ts).getMicros(), Timestamp.parse(te).getMicros(), creds,
                            transaction, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }

        });

        /**
         * GET /record/key/audit?timestamp=<ts>
         * GET /record/key/audit?start=<ts>&end=<te>
         * GET /key/record/audit?timestamp=<ts>
         * GET /key/record/audit?start=<ts>&end=<te>
         */
        get(new Endpoint("/:arg1/:arg2/audit") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String arg2 = getParamValue(":arg2");
                String ts = getParamValue("start");
                String te = getParamValue("end");
                ts = Objects.firstNonNull(ts, getParamValue("timestamp"));
                Object[] args = pickKeyAndRecord(arg1, arg2);
                String key = (String) args[0];
                Long record = (Long) args[1];
                Object data;
                if(ts == null) {
                    data = concourse.auditKeyRecord(key, record, creds,
                            transaction, environment);
                }
                else if(te == null) {
                    data = concourse
                            .auditKeyRecordStart(key, record,
                                    Timestamp.parse(ts).getMicros(), creds, transaction,
                                    environment);
                }
                else {
                    data = concourse.auditKeyRecordStartEnd(key, record,
                            Timestamp.parse(ts).getMicros(), Timestamp.parse(te).getMicros(), creds,
                            transaction, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }
        });

        /**
         * GET /record/key/chronologize?timestamp=<ts>
         * GET /record/key/chronologize?start=<ts>&end=<te>
         * GET /key/record/chronologize?timestamp=<ts>
         * GET /key/record/chronologize?start=<ts>&end=<te>
         */
        get(new Endpoint("/:arg1/:arg2/chronologize") {

            @Override
            protected JsonElement serve() throws Exception {
                String arg1 = getParamValue(":arg1");
                String arg2 = getParamValue(":arg2");
                String ts = getParamValue("start");
                String te = getParamValue("end");
                ts = Objects.firstNonNull(ts, getParamValue("timestamp"));
                Object[] args = pickKeyAndRecord(arg1, arg2);
                String key = (String) args[0];
                Long record = (Long) args[1];
                Object data;
                if(ts == null) {
                    data = concourse.chronologizeKeyRecord(key, record, creds,
                            transaction, environment);
                }
                else if(te == null) {
                    data = concourse
                            .chronologizeKeyRecordStart(key, record,
                                    Timestamp.parse(ts).getMicros(), creds, transaction,
                                    environment);
                }
                else {
                    data = concourse.chronologizeKeyRecordStartEnd(key, record,
                            Timestamp.parse(ts).getMicros(), Timestamp.parse(te).getMicros(), creds,
                            transaction, environment);
                }
                return DataServices.gson().toJsonTree(data);
            }
        });

    }
}
