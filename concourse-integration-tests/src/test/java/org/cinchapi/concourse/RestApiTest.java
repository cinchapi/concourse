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
import java.lang.reflect.Type;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.util.List;
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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
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
     * Return the response body as the appropriate Java object.
     * 
     * @param response
     * @return the response body
     */
    private static <T> T bodyAsJava(Response response, TypeToken<T> type) {
        Type type0 = type.getType();
        return new Gson().fromJson(bodyAsJson(response), type0);
    }
    
    /**
     * Return a JsonElement representation of the response body.
     * 
     * @param response
     * @param the json response
     */
    private static JsonElement bodyAsJson(Response response) {
        try {
            String body = response.body().string();
            JsonElement json = new JsonParser().parse(body);
            return json;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
    /**
     * JSON media type
     */
    private static final MediaType JSON = MediaType
            .parse("application/json; charset=utf-8");
    private String base = "http://localhost:";

    private OkHttpClient http = new OkHttpClient();

    private HttpServer httpServer;

    {
        CookieManager cm = new CookieManager();
        cm.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
        http.setCookieHandler(cm);
    }

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
    public void testGetRoot(){
        login();
        List<Long> r1 = bodyAsJava(get("/"), new TypeToken<List<Long>>(){});
        Assert.assertTrue(r1.isEmpty());
        long record = client.add("foo", "bar");
        List<Long> r2 = bodyAsJava(get("/"), new TypeToken<List<Long>>(){});
        Assert.assertTrue(r2.contains(record));
    }

    @Test
    public void testLogin() throws IOException {
        Response resp = login();
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) new JsonParser().parse(resp.body()
                .string());
        Assert.assertEquals("default", body.get("environment").getAsString());
    }

    @Test
    public void testLoginNonDefaultEnvironment() throws IOException {
        String environment = TestData.getStringNoDigits().replaceAll(" ", "");
        Response resp = login(environment);
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) new JsonParser().parse(resp.body()
                .string());
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
        Response resp = post(environment + "/login", creds.toString());
        return resp;
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
