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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.http.Resource;
import org.cinchapi.concourse.server.http.HttpRequests;
import org.cinchapi.concourse.server.http.HttpArgs;
import org.cinchapi.concourse.server.http.Router;
import org.cinchapi.concourse.server.http.errors.BadLoginSyntaxError;
import org.cinchapi.concourse.thrift.AccessToken;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.DataServices;
import org.cinchapi.concourse.util.ObjectUtils;

import spark.Request;
import spark.Response;

import com.google.common.base.Preconditions;
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
     * Construct a new instance.
     * 
     * @param concourse
     */
    public IndexRouter(ConcourseServer concourse) {
        super(concourse);
    }

    /**
     * POST /login
     */
    public final Resource postLogin = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            JsonElement body = request.bodyAsJson();
            JsonObject credentials;
            if(body.isJsonObject()
                    && (credentials = (JsonObject) body).has("username")
                    && credentials.has("password")) {
                ByteBuffer username = ByteBuffers.fromString(credentials.get(
                        "username").getAsString());
                ByteBuffer password = ByteBuffers.fromString(credentials.get(
                        "password").getAsString());
                AccessToken access = concourse.login(username, password,
                        environment);
                String token = HttpRequests.encodeAuthToken(access,
                        environment, request);
                response.cookie("/", GlobalState.HTTP_AUTH_TOKEN_COOKIE, token,
                        900, false);
                JsonObject data = new JsonObject();
                data.add("token", new JsonPrimitive(token));
                data.add("environment", new JsonPrimitive(environment));

                return data;
            }
            else {
                throw BadLoginSyntaxError.INSTANCE;
            }
        }

    };

    /**
     * POST /logout
     */
    public final Resource postLogout = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            concourse.logout(creds, environment);
            response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
            return NO_DATA;
        }

    };

    /**
     * GET /stage
     */
    public final Resource getStage = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            transaction = concourse.stage(creds, environment);
            String token = Long.toString(transaction.timestamp);
            response.cookie("/", GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE,
                    token, 900, false);
            JsonObject data = new JsonObject();
            data.addProperty("transaction", token);
            return data;
        }

    };

    /**
     * GET /commit
     */
    public final Resource getCommit = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            boolean result = concourse.commit(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return new JsonPrimitive(result);
        }

    };

    /**
     * GET /abort
     */
    public final Resource getAbort = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            concourse.abort(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return NO_DATA;
        }

    };

    /**
     * GET /
     */
    public Resource get = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String ccl = request.getParamValue("query");
            Object data;
            if(!Strings.isNullOrEmpty(ccl)) {
                List<String> keys = request.getParamValues("select");
                String ts = request.getParamValue("timestamp");
                Long timestamp = ts != null ? Longs.tryParse(ts) : null;
                if(keys.isEmpty()) {
                    data = timestamp == null ? concourse.selectCcl(ccl, creds,
                            transaction, environment) : concourse
                            .selectCclTime(ccl, timestamp, creds, transaction,
                                    environment);
                }
                else {
                    data = timestamp == null ? concourse.selectKeysCcl(keys,
                            ccl, creds, transaction, environment) : concourse
                            .selectKeysCclTime(keys, ccl, timestamp, creds,
                                    transaction, environment);
                }
            }
            else {
                data = concourse.find(creds, null, environment);
            }
            return DataServices.gson().toJsonTree(data);
        }

    };

    /**
     * POST /
     * PUT /
     */
    public final Resource upsert = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String json = request.body();
            Set<Long> records = concourse.insertJson(json, creds, transaction,
                    environment);
            return DataServices.gson().toJsonTree(records);
        }

    };

    /**
     * GET /:arg1
     */
    public final Resource get$Arg1 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null : Timestamp.parse(ts)
                    .getMicros();
            Long record = Longs.tryParse(arg1);
            Object data;
            if(record != null) {
                data = timestamp == null ? concourse.selectRecord(record,
                        creds, null, environment) : concourse.selectRecordTime(
                        record, timestamp, creds, transaction, environment);
            }
            else {
                data = timestamp == null ? concourse.browseKey(arg1, creds,
                        null, environment) : concourse.browseKeyTime(arg1,
                        timestamp, creds, transaction, environment);
            }
            return DataServices.gson().toJsonTree(data);
        }

    };

    /**
     * POST /:arg1
     * PUT /:arg1
     */
    public final Resource upsert$Arg1 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
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
                result = concourse.addKeyValue(arg1, value, creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(result);
        }

    };

    /**
     * DELETE /:record
     */
    public final Resource delete$Record = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            long record = Long.parseLong(request.getParamValue(":record"));
            concourse.clearRecord(record, creds, transaction, environment);
            return NO_DATA;
        }

    };

    /**
     * GET /record/audit?timestamp=<ts>
     * GET /record/audit?start=<ts>&end=<te>
     */
    public final Resource get$RecordAudit = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":record");
            String start = request.getParamValue("start");
            String end = request.getParamValue("end");
            start = ObjectUtils.firstNonNull(start,
                    request.getParamValue("timestamp"));
            Long record = Longs.tryParse(arg1);
            Object data;
            Preconditions.checkArgument(record != null,
                    "Cannot perform audit on %s because it "
                            + "is not a valid record", arg1);
            if(start != null && end != null) {
                data = concourse.auditRecordStartEnd(record,
                        Timestamp.parse(start).getMicros(), Timestamp
                                .parse(end).getMicros(), creds, transaction,
                        environment);
            }
            else if(start != null) {
                data = concourse.auditRecordStart(record, Timestamp
                        .parse(start).getMicros(), creds, transaction,
                        environment);
            }
            else {
                data = concourse.auditRecord(record, creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(data);
        }

    };

    /**
     * GET /key/record[?timestamp=<ts>]
     * GET /record/key[?timestamp=<ts>]
     */
    public final Resource get$Arg1$Arg2 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null : Timestamp.parse(ts)
                    .getMicros();
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            Object data;
            if(timestamp == null) {
                data = concourse.selectKeyRecord(key, record, creds,
                        transaction, environment);
            }
            else {
                data = concourse.selectKeyRecordTime(key, record, timestamp,
                        creds, transaction, environment);
            }
            return DataServices.gson().toJsonTree(data);
        }

    };

    /**
     * POST /record/key
     * POST /key/record
     */
    public final Resource post$Arg1$Arg2 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            TObject value = Convert.javaToThrift(Convert.stringToJava(request
                    .body()));
            boolean result = concourse.addKeyValueRecord(key, value, record,
                    creds, transaction, environment);
            return DataServices.gson().toJsonTree(result);
        }

    };

    /**
     * PUT /record/key
     * PUT /key/record
     */
    public final Resource put$Arg1$Arg2 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            TObject value = Convert.javaToThrift(Convert.stringToJava(request
                    .body()));
            concourse.setKeyValueRecord(key, value, record, creds, transaction,
                    environment);
            return NO_DATA;
        }

    };

    /**
     * DELETE /record/key
     * DELETE /key/record
     */
    public final Resource delete$Arg1$Arg2 = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            HttpArgs args = HttpArgs.parse(request.getParamValue(":arg1"),
                    request.getParamValue(":arg2"));
            String key = args.getKey();
            Long record = args.getRecord();
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

    };

    /**
     * GET /record/key/audit?timestamp=<ts>
     * GET /record/key/audit?start=<ts>&end=<te>
     * GET /key/record/audit?timestamp=<ts>
     * GET /key/record/audit?start=<ts>&end=<te>
     */
    public final Resource get$Arg1$Arg2Audit = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValueOrAlias("start", "timestamp");
            String end = request.getParamValue("end");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            Preconditions.checkArgument(
                    record != null && !StringUtils.isBlank(key),
                    "Cannot perform audit on %s/%s because it "
                            + "is not a valid key/record combination", arg1,
                    arg2);
            Object data = null;
            if(start != null && end != null) {
                data = concourse.auditKeyRecordStartEnd(key, record, Timestamp
                        .parse(start).getMicros(), Timestamp.parse(end)
                        .getMicros(), creds, transaction, environment);
            }
            else if(start != null) {
                data = concourse.auditKeyRecordStart(key, record, Timestamp
                        .parse(start).getMicros(), creds, transaction,
                        environment);
            }
            else {
                data = concourse.auditKeyRecord(key, record, creds,
                        transaction, environment);
            }
            return DataServices.gson().toJsonTree(data);

        }

    };

    /**
     * GET /record/key/chronologize?timestamp=<ts>
     * GET /record/key/chronologize?start=<ts>&end=<te>
     * GET /key/record/chronologize?timestamp=<ts>
     * GET /key/record/chronologize?start=<ts>&end=<te>
     */
    public final Resource get$Arg1$Arg2Chronologize = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValueOrAlias("start", "timestamp");
            String end = request.getParamValue("end");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            Object data = null;
            if(start == null) {
                data = concourse.chronologizeKeyRecord(key, record, creds,
                        transaction, environment);
            }
            else if(end == null) {
                data = concourse.chronologizeKeyRecordStart(key, record,
                        Timestamp.parse(start).getMicros(), creds, transaction,
                        environment);
            }
            else {
                data = concourse.chronologizeKeyRecordStartEnd(key, record,
                        Timestamp.parse(start).getMicros(), Timestamp
                                .parse(end).getMicros(), creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(data);
        }

    };

    /**
     * GET /record/key/revert?timestamp=<ts>
     * GET /key/record/revert?timestamp=<ts>
     */
    public final Resource get$Arg1$Arg2Revert = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String ts = request.getParamValue("timestamp");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            if(key != null && record != null) {
                concourse.revertKeyRecordTime(key, record.longValue(),
                        Timestamp.parse(ts).getMicros(), creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(true);
        }

    };

    /**
     * GET /record/key/diff?start=<ts>&end=<te>
     * GET /key/record/diff?start=<ts>
     * GET /record/diff?start=<ts>&end=<te>
     * GET /key/diff?start=<ts>&end=<te>
     * 
     * @return
     */
    public final Resource get$Arg1$Arg2Diff = new Resource() {

        @Override
        protected JsonElement serve(Request request, Response response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValue("start");
            String end = request.getParamValue("end");
            HttpArgs args = HttpArgs.parse(arg1, arg2);
            String key = args.getKey();
            Long record = args.getRecord();
            Object data = null;
            if(key != null && record != null && start != null & end != null) {
                data = concourse.diffKeyRecordStartEnd(key, record, Timestamp
                        .parse(start).getMicros(), Timestamp.parse(end)
                        .getMicros(), creds, transaction, environment);
            }
            else if(key != null && record != null && start != null
                    & end == null) {
                data = concourse.diffKeyRecordStart(key, record, Timestamp
                        .parse(start).getMicros(), creds, transaction,
                        environment);
            }
            else if(key == null && record != null && start != null
                    & end != null) {
                data = concourse.diffRecordStartEnd(record,
                        Timestamp.parse(start).getMicros(), Timestamp
                                .parse(end).getMicros(), creds, transaction,
                        environment);
            }
            else if(key != null && record == null && start != null
                    & end != null) {
                data = concourse.diffKeyStartEnd(key, Timestamp.parse(start)
                        .getMicros(), Timestamp.parse(end).getMicros(), creds,
                        transaction, environment);
            }

            return DataServices.gson().toJsonTree(data);
        }

    };

}
