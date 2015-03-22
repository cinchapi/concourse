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
package org.cinchapi.concourse;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.http.HttpServer;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Networking;
import org.cinchapi.concourse.util.Reflection;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * Unit tests for the REST API.
 * 
 * @author Jeff Nelson
 */
public class RestApiTest extends ConcourseIntegrationTest {

    /**
     * JSON media type
     */
    private static final MediaType JSON = MediaType
            .parse("application/json; charset=utf-8");
    private String base = "http://localhost:";
    private OkHttpClient http = new OkHttpClient();

    private HttpServer httpServer;

    @Override
    public void afterEachTest() {
        httpServer.stop();
    }

    @Override
    public void beforeEachTest() {
        int port = Networking.getOpenPort();
        httpServer = HttpServer.create(
                Reflection.<ConcourseServer> get("server", this), port);
        httpServer.start();
        // Wait for the HTTP server to start
        Request req = new Request.Builder().url(base).head().build();
        long start = Time.now();
        boolean escape = false;
        while (!escape) {
            try {
                http.newCall(req).execute();
                escape = true;
            }
            catch (IOException e) {
                escape = TimeUnit.SECONDS.convert(Time.now() - start,
                        TimeUnit.MICROSECONDS) > 5;
            }
        }
        base += port;
    }

    @Test
    public void testLogin() throws IOException {
        Response resp = login();
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) new JsonParser().parse(resp.body().string());
        Assert.assertEquals("default", body.get("environment").getAsString());
    }
    
    @Test
    public void testLoginNonDefaultEnvironment() throws IOException {
        String environment = TestData.getStringNoDigits().replaceAll(" ", "");
        Response resp = login(environment);
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) new JsonParser().parse(resp.body().string());
        Assert.assertEquals(environment, body.get("environment").getAsString());
    }

    /**
     * Perform a GET request
     * 
     * @param route
     * @return the response
     */
    protected Response get(String route) {
        try {
            return http.newCall(
                    new Request.Builder().url(base + route).get().build())
                    .execute();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Perform a login request to the default environment.
     * 
     * @return the response
     */
    protected Response login() {
        return login("");
    }

    /**
     * Perform a login request.
     * 
     * @param environment
     * @return the response
     */
    protected Response login(String environment) {
        environment = Strings.isNullOrEmpty(environment) ? "" : "/"
                + environment;
        JsonObject creds = new JsonObject();
        creds.addProperty("username", "admin");
        creds.addProperty("password", "admin");
        return post(environment + "/login", creds.toString());
    }

    /**
     * Perform a POST request.
     * 
     * @param route
     * @param data
     * @return the response
     */
    protected Response post(String route, String data) {
        try {
            RequestBody body = RequestBody.create(JSON, data);
            return http.newCall(
                    new Request.Builder().url(base + route).post(body).build())
                    .execute();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
