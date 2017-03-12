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
package com.cinchapi.concourse.http;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.http.HttpServer;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Networking;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * A class that defines methods and basic tests for interacting with the HTTP
 * functionality of Concourse Server.
 * 
 * @author Jeff Nelson
 */
public class HttpTest extends ConcourseIntegrationTest {

    /**
     * Return the response body as the appropriate Java object.
     * 
     * @param response
     * @return the response body
     */
    protected static <T> T bodyAsJava(Response response, TypeToken<T> type) {
        Type type0 = type.getType();
        return new Gson().fromJson(bodyAsJson(response), type0);
    }

    /**
     * Return a JsonElement representation of the response body.
     * 
     * @param response
     * @param the json response
     */
    protected static JsonElement bodyAsJson(Response response) {
        try {
            String body = response.body().string();
            JsonElement json = new JsonParser().parse(body);
            Variables.register("json_body_" + response.hashCode(), body);
            return json;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Do anything that is necessary to clean up the URL args. For example, make
     * sure any records (represented as an int or long) does not get rendered
     * using comma separators.
     * 
     * @param args
     * @return the clean args
     */
    private static Object[] cleanUrlArgs(Object... args) {
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if(arg.getClass() == long.class || arg.getClass() == int.class) {
                args[i] = Long.toString((long) arg);
            }
            else if(arg.getClass() == Long.class
                    || arg.getClass() == Integer.class) {
                args[i] = arg.toString();
            }
        }
        return args;
    }

    /**
     * Go through the {@code args} and filter out any that shouldn't be
     * considered URL args. It is possible that some of the filtered args will
     * be passed as arguments to the {@code builder}.
     * 
     * @param builder
     * @param args
     * @return the filtered args
     */
    private static Object[] filterArgs(Request.Builder builder, Object... args) {
        List<Object> argsList = Lists.newArrayList(args);
        Iterator<Object> it = argsList.iterator();
        while (it.hasNext()) {
            Object arg = it.next();
            if(arg instanceof Headers) {
                builder.headers((Headers) arg);
                it.remove();
            }
        }
        args = argsList.size() != args.length ? argsList.toArray() : args;
        return args;
    }

    /**
     * JSON media type
     */
    private static final MediaType JSON = MediaType
            .parse("application/json; charset=utf-8");

    /**
     * The base URL.
     */
    private String base = "http://localhost:";

    /**
     * The HTTP Client.
     */
    private OkHttpClient http = new OkHttpClient();

    /**
     * A reference to the HttpServer that interacts with Concourse Server.
     * Subclasses should never need to interact with this directly.
     */
    private HttpServer httpServer;

    {
        // Setup support for cookies
        CookieManager cm = new CookieManager();
        cm.setCookiePolicy(CookiePolicy.ACCEPT_ALL);
        http.setCookieHandler(cm);
    }

    @Override
    public void afterEachTest() {
        httpServer.stop();
        clearClientCookies();
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

    /**
     * Remove all the client side cookies.
     * 
     * @return {@code true} if the cookies are removed
     */
    protected boolean clearClientCookies() {
        return ((CookieManager) http.getCookieHandler()).getCookieStore()
                .removeAll();
    }

    /**
     * Perform a DELETE request
     * 
     * @param route
     * @param args - include a {@link Headers} object to set the request headers
     * @return the response
     */
    protected Response delete(String route, Object... args) {
        try {
            Request.Builder builder = new Request.Builder();
            args = filterArgs(builder, args);
            args = cleanUrlArgs(args);
            route = MessageFormat.format(route, args);
            Request request = builder.url(base + route).delete().build();
            Response response = http.newCall(request).execute();
            long ts = response.hashCode();
            Variables.register("request_" + ts, request);
            Variables.register("response_" + ts, response);
            return response;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Perform a GET request
     * 
     * @param route
     * @param args - include a {@link Headers} object to set the request headers
     * @return the response
     */
    protected Response get(String route, Object... args) {
        try {
            Request.Builder builder = new Request.Builder();
            args = filterArgs(builder, args);
            args = cleanUrlArgs(args);
            route = MessageFormat.format(route, args);
            Request request = builder.url(base + route).get().build();
            Response response = http.newCall(request).execute();
            long ts = response.hashCode();
            Variables.register("request_" + ts, request);
            Variables.register("response_" + ts, response);
            return response;
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
     * @param args - include a {@link Headers} object to set the request headers
     * @return the response
     */
    protected Response post(String route, String data, Object... args) {
        try {
            RequestBody body = RequestBody.create(JSON, data);
            Request.Builder builder = new Request.Builder();
            args = filterArgs(builder, args);
            args = cleanUrlArgs(args);
            route = MessageFormat.format(route, args);
            Request request = builder.url(base + route).post(body).build();
            Response response = http.newCall(request).execute();
            long ts = response.hashCode();
            Variables.register("request_" + ts, request);
            Variables.register("response_" + ts, response);
            return response;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Perform a PUT request.
     * 
     * @param route
     * @param data
     * @param args - include a {@link Headers} object to set the request headers
     * @return the response
     */
    protected Response put(String route, String data, Object... args) {
        try {
            RequestBody body = RequestBody.create(JSON, data);
            Request.Builder builder = new Request.Builder();
            args = filterArgs(builder, args);
            args = cleanUrlArgs(args);
            route = MessageFormat.format(route, args);
            Request request = builder.url(base + route).put(body).build();
            Response response = http.newCall(request).execute();
            long ts = response.hashCode();
            Variables.register("request_" + ts, request);
            Variables.register("response_" + ts, response);
            return response;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
