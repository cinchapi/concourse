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
package com.cinchapi.concourse.server.http.plugins;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.concourse.lang.NaturalLanguage;
import com.cinchapi.concourse.plugin.http.Endpoint;
import com.cinchapi.concourse.plugin.http.HttpPlugin;
import com.cinchapi.concourse.plugin.http.HttpRequest;
import com.cinchapi.concourse.plugin.http.HttpResponse;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.HttpArgs;
import com.cinchapi.concourse.server.http.HttpRequests;
import com.cinchapi.concourse.server.http.errors.BadLoginSyntaxError;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.DataServices;
import com.cinchapi.concourse.util.ObjectUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * The core/default router.
 * 
 * @author Jeff Nelson
 */
public class IndexRouter extends HttpPlugin {

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
    public final Endpoint postLogin = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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

                return data.toString();
            }
            else {
                throw BadLoginSyntaxError.INSTANCE;
            }
        }

    };

    /**
     * POST /logout
     */
    public final Endpoint postLogout = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            concourse.logout(creds, environment);
            response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
            return NO_DATA.toString();
        }

    };

    /**
     * GET /stage
     */
    public final Endpoint getStage = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            transaction = concourse.stage(creds, environment);
            String token = Long.toString(transaction.timestamp);
            response.cookie("/", GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE,
                    token, 900, false);
            JsonObject data = new JsonObject();
            data.addProperty("transaction", token);
            return data.toString();
        }

    };

    /**
     * GET /commit
     */
    public final Endpoint getCommit = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            boolean result = concourse.commit(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return new JsonPrimitive(result).toString();
        }

    };

    /**
     * GET /abort
     */
    public final Endpoint getAbort = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            concourse.abort(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return NO_DATA.toString();
        }

    };

    /**
     * GET /
     */
    public Endpoint get = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                data = concourse.inventory(creds, null, environment);
            }
            return new Gson().toJsonTree(data).toString();
        }

    };

    /**
     * POST /
     * PUT /
     */
    public final Endpoint upsert = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String json = request.body();
            Set<Long> records = concourse.insertJson(json, creds, transaction,
                    environment);
            return DataServices.gson().toJsonTree(records).toString();
        }

    };

    /**
     * GET /:arg1
     */
    public final Endpoint get$Arg1 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null : NaturalLanguage
                    .parseMicros(ts);
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
            return DataServices.gson().toJsonTree(data).toString();
        }

    };

    /**
     * POST /:arg1
     * PUT /:arg1
     */
    public final Endpoint upsert$Arg1 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
            return DataServices.gson().toJsonTree(result).toString();
        }

    };

    /**
     * DELETE /:record
     */
    public final Endpoint delete$Record = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            long record = Long.parseLong(request.getParamValue(":record"));
            concourse.clearRecord(record, creds, transaction, environment);
            return NO_DATA.toString();
        }

    };

    /**
     * GET /record/audit?timestamp=<ts>
     * GET /record/audit?start=<ts>&end=<te>
     */
    public final Endpoint get$RecordAudit = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String arg1 = request.getParamValue(":record");
            String start = request.getParamValue("start");
            String end = request.getParamValue("end");
            start = ObjectUtils.firstNonNullOrNull(start,
                    request.getParamValue("timestamp"));
            Long record = Longs.tryParse(arg1);
            Object data;
            Preconditions.checkArgument(record != null,
                    "Cannot perform audit on %s because it "
                            + "is not a valid record", arg1);
            if(start != null && end != null) {
                data = concourse.auditRecordStartEnd(record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(start != null) {
                data = concourse.auditRecordStart(record,
                        NaturalLanguage.parseMicros(start), creds, transaction,
                        environment);
            }
            else {
                data = concourse.auditRecord(record, creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(data).toString();
        }

    };

    /**
     * GET /key/record[?timestamp=<ts>]
     * GET /record/key[?timestamp=<ts>]
     */
    public final Endpoint get$Arg1$Arg2 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
                AccessToken creds, TransactionToken transaction,
                String environment) throws Exception {
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null : NaturalLanguage
                    .parseMicros(ts);
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
            return DataServices.gson().toJsonTree(data).toString();
        }

    };

    /**
     * POST /record/key
     * POST /key/record
     */
    public final Endpoint post$Arg1$Arg2 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
            return DataServices.gson().toJsonTree(result).toString();
        }

    };

    /**
     * PUT /record/key
     * PUT /key/record
     */
    public final Endpoint put$Arg1$Arg2 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
            return NO_DATA.toString();
        }

    };

    /**
     * DELETE /record/key
     * DELETE /key/record
     */
    public final Endpoint delete$Arg1$Arg2 = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                return NO_DATA.toString();
            }
            else {
                TObject value = Convert.javaToThrift(Convert
                        .stringToJava(request.body()));
                Object data = concourse.removeKeyValueRecord(key, value,
                        record, creds, transaction, environment);
                return DataServices.gson().toJsonTree(data).toString();
            }
        }

    };

    /**
     * GET /record/key/audit?timestamp=<ts>
     * GET /record/key/audit?start=<ts>&end=<te>
     * GET /key/record/audit?timestamp=<ts>
     * GET /key/record/audit?start=<ts>&end=<te>
     */
    public final Endpoint get$Arg1$Arg2Audit = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                data = concourse.auditKeyRecordStartEnd(key, record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(start != null) {
                data = concourse.auditKeyRecordStart(key, record,
                        NaturalLanguage.parseMicros(start), creds, transaction,
                        environment);
            }
            else {
                data = concourse.auditKeyRecord(key, record, creds,
                        transaction, environment);
            }
            return DataServices.gson().toJsonTree(data).toString();

        }

    };

    /**
     * GET /record/key/chronologize?timestamp=<ts>
     * GET /record/key/chronologize?start=<ts>&end=<te>
     * GET /key/record/chronologize?timestamp=<ts>
     * GET /key/record/chronologize?start=<ts>&end=<te>
     */
    public final Endpoint get$Arg1$Arg2Chronologize = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                        NaturalLanguage.parseMicros(start), creds, transaction,
                        environment);
            }
            else {
                data = concourse.chronologizeKeyRecordStartEnd(key, record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(data).toString();
        }

    };

    /**
     * GET /record/key/revert?timestamp=<ts>
     * GET /key/record/revert?timestamp=<ts>
     */
    public final Endpoint get$Arg1$Arg2Revert = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                        NaturalLanguage.parseMicros(ts), creds, transaction,
                        environment);
            }
            return DataServices.gson().toJsonTree(true).toString();
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
    public final Endpoint get$Arg1$Arg2Diff = new Endpoint() {

        @Override
        public String serve(HttpRequest request, HttpResponse response,
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
                data = concourse.diffKeyRecordStartEnd(key, record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(key != null && record != null && start != null
                    & end == null) {
                data = concourse.diffKeyRecordStart(key, record,
                        NaturalLanguage.parseMicros(start), creds, transaction,
                        environment);
            }
            else if(key == null && record != null && start != null
                    & end != null) {
                data = concourse.diffRecordStartEnd(record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(key != null && record == null && start != null
                    & end != null) {
                data = concourse.diffKeyStartEnd(key,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }

            return DataServices.gson().toJsonTree(data).toString();
        }

    };

}
