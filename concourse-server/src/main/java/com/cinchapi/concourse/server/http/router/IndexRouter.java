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
package com.cinchapi.concourse.server.http.router;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.concourse.lang.NaturalLanguage;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.RouteArgs;
import com.cinchapi.concourse.server.http.EndpointContainer;
import com.cinchapi.concourse.server.http.HttpRequest;
import com.cinchapi.concourse.server.http.HttpRequests;
import com.cinchapi.concourse.server.http.HttpResponse;
import com.cinchapi.concourse.server.http.JsonEndpoint;
import com.cinchapi.concourse.server.http.errors.BadLoginSyntaxError;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.ObjectUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;

/**
 * The core/default router.
 * 
 * @author Jeff Nelson
 */
public class IndexRouter extends EndpointContainer {

    /**
     * @apiDefine LoginApi
     * @apiGroup Auth
     * @apiParam {String} username the username with which to connect
     * @apiParam {String} password the password with which to authenticate
     * @apiParamExample {json} Sample Credentials:
     *                  {"username": "admin", "password": "admin"}
     * @apiSuccess (200) {String} token an authentication token
     * @apiSuccess (200) {String} environment the name of the environment to
     *             which the
     *             token is associated
     * @apiSuccessExample {json} Successful Response:
     *                    {
     *                    "token":
     *                    "V6CzvyRSMEVJM-ECdY1E_MweUA4gGY3o1JwzzmgnvA61xFRpeJWUQVYMQZp417o8hCOfCI-LhgtkKdckz7V3uNUliDphyE28wJbKmEx2ZFh9FfrOnTIx76NJ-FEBxOCMwu5KGBf4UuItutHv8gWqnw=="
     *                    ,
     *                    "environment": "default"
     *                    }
     * @apiError (401) Unauthorized The <code>username</code>/
     *           <code>password</code> combination is invalid
     */
    /**
     * @api {post} /login Login to default environment
     * @apiDescription Provide a JSON object containing credentials to login to
     *                 the default environment of Concourse Server and, if
     *                 successful, receive an auth token for further
     *                 interaction. All core and plugin API endpoints must be
     *                 preceded by a login call. The provided auth token is
     *                 stored in a <code>concourse_db_auth_token</code> cookie;
     *                 however, it is recommended that you supply it using the
     *                 <code>X-Auth-Token-Header</code> on subsequent requests.
     * @apiGroup Auth
     * @apiName Login
     * @apiUse LoginApi
     */
    /**
     * @api {post} /:environment/login Login to specific environment
     * @apiDescription An alternative login endpoint to connect to a specific
     *                 environment. Same rules apply: provide a JSON object
     *                 containing credentials to login to Concourse Server and,
     *                 if successful, receive an auth token for further
     *                 interaction. All core and plugin API endpoints must be
     *                 preceded by a login call. The provided auth token is
     *                 stored in a <code>concourse_db_auth_token</code> cookie;
     *                 however, it is recommended that you supply it using the
     *                 <code>X-Auth-Token-Header</code> on subsequent requests.
     * @apiGroup Auth
     * @apiName LoginEnvironment
     * @apiParam {String} environment the name of the environment to which the
     *           connection should be made
     * @apiUse LoginApi
     * @apiVersion 0.5.0
     */
    public final JsonEndpoint postLogin = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            try {
                JsonElement body = request.bodyAsJson();
                JsonObject credentials;
                if(body.isJsonObject()
                        && (credentials = (JsonObject) body).has("username")
                        && credentials.has("password")) {
                    ByteBuffer username = ByteBuffers.fromString(
                            credentials.get("username").getAsString());
                    ByteBuffer password = ByteBuffers.fromString(
                            credentials.get("password").getAsString());
                    AccessToken access = concourse.login(username, password,
                            environment);
                    String token = HttpRequests.encodeAuthToken(access,
                            environment, request);
                    response.cookie("/", GlobalState.HTTP_AUTH_TOKEN_COOKIE,
                            token, 900, false);
                    Map<String, Object> payload = Maps
                            .newHashMapWithExpectedSize(2);
                    payload.put("token", token);
                    payload.put("environment", environment);
                    return payload;
                }
            }
            catch (JsonSyntaxException e) {}
            throw BadLoginSyntaxError.INSTANCE;

        }

    };

    /**
     * GET /record/audit?timestamp=<ts>
     * GET /record/audit?start=<ts>&end=<te>
     */
    public final JsonEndpoint get$RecordAudit = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":record");
            String start = request.getParamValue("start");
            String end = request.getParamValue("end");
            start = ObjectUtils.firstNonNullOrNull(start,
                    request.getParamValue("timestamp"));
            Long record = Longs.tryParse(arg1);
            Object data;
            Preconditions.checkArgument(record != null,
                    "Cannot perform audit on %s because it "
                            + "is not a valid record",
                    arg1);
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
            return data;
        }

    };

    /**
     * @apiDefine AuthHeader
     * @apiHeader {String} X-Auth-Token the unique token that is returned from
     *            the <code>/login</code> endpoint
     * @apiError (401) Unauthorized the provided auth token is invalid
     */

    /**
     * DELETE /record/key
     * DELETE /key/record
     */
    public final JsonEndpoint delete$Arg1$Arg2 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            RouteArgs args = RouteArgs.parse(request.getParamValue(":arg1"),
                    request.getParamValue(":arg2"));
            String key = args.key();
            Long record = args.record();
            String body = request.body();
            if(StringUtils.isBlank(body)) {
                concourse.clearKeyRecord(key, record, creds, transaction,
                        environment);
                return NO_DATA;
            }
            else {
                TObject value = Convert
                        .javaToThrift(Convert.stringToJava(request.body()));
                Object data = concourse.removeKeyValueRecord(key, value, record,
                        creds, transaction, environment);
                return data;
            }
        }

    };

    /**
     * DELETE /:record
     */
    public final JsonEndpoint delete$Record = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            long record = Long.parseLong(request.getParamValue(":record"));
            concourse.clearRecord(record, creds, transaction, environment);
            return NO_DATA;
        }

    };

    /**
     * @api {get} / Return a list of all record ids
     * @apiDescription Return all the records that have current or historical
     *                 data as a JSON array containing record ids. This endpoint
     *                 is analogous to the following driver methods:
     *                 <ul>
     *                 <li><code>inventory()</code></li>
     *                 </ul>
     * 
     * @apiName Inventory
     * @apiGroup Reading
     * @apiUse AuthHeader
     */
    /**
     * @api {get} / Select all the data from records that match a criteria.
     * @apiDescription Select all the data from records that match a criteria as
     *                 an array of JSON objects. This endpoint
     *                 is analogous to the following driver methods:
     *                 <ul>
     *                 <li><code>select(ccl)</code></li>
     *                 <li><code>select(ccl, timestamp)</code></li>
     *                 </ul>
     * @apiName SelectCcl
     * @apiGroup Reading
     * @apiParam {String} query a CCL statement used to filter matching records
     * @apiParam {StringOrInteger} [timestamp] the historical timestamp to use
     *           when reading the data
     * @apiUse AuthHeader
     */
    /**
     * @api {get} / Select values for specific keys from records criteria.
     * @apiDescription Return all the values stored for each of the specific
     *                 keys that match the criteria. This endpoint
     *                 is analogous to the following driver methods:
     *                 <ul>
     *                 <li><code>select(key, ccl)</code></li>
     *                 <li><code>select(key, ccl, timestamp)</code></li>
     *                 <li><code>select(keys, ccl)</code></li>
     *                 <li><code>select(keys, ccl, timestamp)</code></li>
     *                 </ul>
     * @apiName SelectKeysCcl
     * @apiGroup Reading
     * @apiParam {String} query a CCL statement used to filter matching records
     * @apiParam {String} keys a comma separated list of keys to select from
     *           each of the matching records
     * @apiParam {StringOrInteger} [timestamp] the historical timestamp to use
     *           when reading the data
     * @apiUse AuthHeader
     */
    public JsonEndpoint get = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String ccl = request.getParamValue("query");
            Object data;
            if(!Strings.isNullOrEmpty(ccl)) {
                List<String> keys = request.getParamValues("select");
                String ts = request.getParamValue("timestamp");
                Long timestamp = ts != null ? Longs.tryParse(ts) : null;
                if(keys.isEmpty()) {
                    data = timestamp == null
                            ? concourse.selectCcl(ccl, creds, transaction,
                                    environment)
                            : concourse.selectCclTime(ccl, timestamp, creds,
                                    transaction, environment);
                }
                else {
                    data = timestamp == null
                            ? concourse.selectKeysCcl(keys, ccl, creds,
                                    transaction, environment)
                            : concourse.selectKeysCclTime(keys, ccl, timestamp,
                                    creds, transaction, environment);
                }
                return data;
            }
            else {
                data = concourse.inventory(creds, null, environment);
                return new Gson().toJsonTree(data);
            }

        }

    };

    /**
     * GET /:arg1
     */
    public final JsonEndpoint get$Arg1 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null
                    : NaturalLanguage.parseMicros(ts);
            Long record = Longs.tryParse(arg1);
            Object data;
            if(record != null) {
                data = timestamp == null
                        ? concourse.selectRecord(record, creds, null,
                                environment)
                        : concourse.selectRecordTime(record, timestamp, creds,
                                transaction, environment);
            }
            else {
                data = timestamp == null
                        ? concourse.browseKey(arg1, creds, null, environment)
                        : concourse.browseKeyTime(arg1, timestamp, creds,
                                transaction, environment);
            }
            return data;
        }

    };

    /**
     * GET /key/record[?timestamp=<ts>]
     * GET /record/key[?timestamp=<ts>]
     */
    public final JsonEndpoint get$Arg1$Arg2 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String ts = request.getParamValue("timestamp");
            Long timestamp = ts == null ? null
                    : NaturalLanguage.parseMicros(ts);
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            Object data;
            if(timestamp == null) {
                data = concourse.selectKeyRecord(key, record, creds,
                        transaction, environment);
            }
            else {
                data = concourse.selectKeyRecordTime(key, record, timestamp,
                        creds, transaction, environment);
            }
            return data;
        }

    };

    /**
     * GET /record/key/audit?timestamp=<ts>
     * GET /record/key/audit?start=<ts>&end=<te>
     * GET /key/record/audit?timestamp=<ts>
     * GET /key/record/audit?start=<ts>&end=<te>
     */
    public final JsonEndpoint get$Arg1$Arg2Audit = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValueOrAlias("start", "timestamp");
            String end = request.getParamValue("end");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            Preconditions.checkArgument(
                    record != null && !StringUtils.isBlank(key),
                    "Cannot perform audit on %s/%s because it "
                            + "is not a valid key/record combination",
                    arg1, arg2);
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
                data = concourse.auditKeyRecord(key, record, creds, transaction,
                        environment);
            }
            return data;

        }

    };

    /**
     * GET /record/key/chronologize?timestamp=<ts>
     * GET /record/key/chronologize?start=<ts>&end=<te>
     * GET /key/record/chronologize?timestamp=<ts>
     * GET /key/record/chronologize?start=<ts>&end=<te>
     */
    public final JsonEndpoint get$Arg1$Arg2Chronologize = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValueOrAlias("start", "timestamp");
            String end = request.getParamValue("end");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
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
            return data;
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
    public final JsonEndpoint get$Arg1$Arg2Diff = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String start = request.getParamValue("start");
            String end = request.getParamValue("end");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            Object data = null;
            if(key != null && record != null && start != null & end != null) {
                data = concourse.diffKeyRecordStartEnd(key, record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(key != null && record != null
                    && start != null & end == null) {
                data = concourse.diffKeyRecordStart(key, record,
                        NaturalLanguage.parseMicros(start), creds, transaction,
                        environment);
            }
            else if(key == null && record != null
                    && start != null & end != null) {
                data = concourse.diffRecordStartEnd(record,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }
            else if(key != null && record == null
                    && start != null & end != null) {
                data = concourse.diffKeyStartEnd(key,
                        NaturalLanguage.parseMicros(start),
                        NaturalLanguage.parseMicros(end), creds, transaction,
                        environment);
            }

            return data;
        }

    };

    /**
     * GET /record/key/revert?timestamp=<ts>
     * GET /key/record/revert?timestamp=<ts>
     */
    public final JsonEndpoint get$Arg1$Arg2Revert = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            String ts = request.getParamValue("timestamp");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            if(key != null && record != null) {
                concourse.revertKeyRecordTime(key, record.longValue(),
                        NaturalLanguage.parseMicros(ts), creds, transaction,
                        environment);
            }
            return true;
        }

    };

    /**
     * @api {get} /abort Abort transaction
     * @apiGroup Transactions
     * @apiName Abort
     * @apiUse AuthHeader
     */
    public final JsonEndpoint getAbort = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            concourse.abort(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return NO_DATA;
        }

    };

    /**
     * @api {get} /commit Commit transaction
     * @apiGroup Transactions
     * @apiName Commit
     * @apiSuccess (200) {Boolean} success a boolean that indicates whether the
     *             transaction successfully committed
     * @apiSuccessExample {json} Success-Response:
     *                    true
     * @apiUse AuthHeader
     */
    public final JsonEndpoint getCommit = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            boolean result = concourse.commit(creds, transaction, environment);
            response.removeCookie(GlobalState.HTTP_TRANSACTION_TOKEN_COOKIE);
            return new JsonPrimitive(result);
        }

    };

    /**
     * @api {get} /stage Start transaction
     * @apiDescription Start a new transaction.
     * @apiGroup Transactions
     * @apiName Stage
     * @apiSuccess (200) {String} transaction the transaction id
     * @apiSuccessExample {json} Success-Response:
     *                    {
     *                    "transaction": "1457544886097000"
     *                    }
     * @apiUse AuthHeader
     */
    public final JsonEndpoint getStage = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
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
     * POST /record/key
     * POST /key/record
     */
    public final JsonEndpoint post$Arg1$Arg2 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            TObject value = Convert
                    .javaToThrift(Convert.stringToJava(request.body()));
            boolean result = concourse.addKeyValueRecord(key, value, record,
                    creds, transaction, environment);
            return result;
        }

    };

    /**
     * @api {post} /logout Logout
     * @apiDescription End the current session. Afterwards, the provided auth
     *                 token will no longer work.
     * @apiGroup Auth
     * @apiName Logout
     * @apiUse AuthHeader
     */
    public final JsonEndpoint postLogout = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            concourse.logout(creds, environment);
            response.removeCookie(GlobalState.HTTP_AUTH_TOKEN_COOKIE);
            return NO_DATA;
        }

    };

    /**
     * PUT /record/key
     * PUT /key/record
     */
    public final JsonEndpoint put$Arg1$Arg2 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            String arg2 = request.getParamValue(":arg2");
            RouteArgs args = RouteArgs.parse(arg1, arg2);
            String key = args.key();
            Long record = args.record();
            TObject value = Convert
                    .javaToThrift(Convert.stringToJava(request.body()));
            concourse.setKeyValueRecord(key, value, record, creds, transaction,
                    environment);
            return NO_DATA;
        }

    };

    /**
     * POST /
     * PUT /
     */
    public final JsonEndpoint upsert = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String json = request.body();
            Set<Long> records = concourse.insertJson(json, creds, transaction,
                    environment);
            return records;
        }

    };

    /**
     * POST /:arg1
     * PUT /:arg1
     */
    public final JsonEndpoint upsert$Arg1 = new JsonEndpoint() {

        @Override
        public Object serve(HttpRequest request, AccessToken creds,
                TransactionToken transaction, String environment,
                HttpResponse response) throws Exception {
            String arg1 = request.getParamValue(":arg1");
            Long record = Longs.tryParse(arg1);
            Object result;
            if(record != null) {
                String json = request.body();
                result = concourse.insertJsonRecord(json, record, creds,
                        transaction, environment);
            }
            else {
                TObject value = Convert
                        .javaToThrift(Convert.stringToJava(request.body()));
                result = concourse.addKeyValue(arg1, value, creds, transaction,
                        environment);
            }
            return result;
        }

    };

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public IndexRouter(ConcourseServer concourse) {
        super(concourse);
    }

    @Override
    public int getWeight() {
        return Integer.MAX_VALUE;
    }

}
