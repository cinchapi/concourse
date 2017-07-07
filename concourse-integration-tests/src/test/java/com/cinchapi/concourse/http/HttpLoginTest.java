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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.http.errors.BadLoginSyntaxError;
import com.cinchapi.concourse.util.TestData;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.Response;

/**
 * Unit tests for logging into Concourse Server via HTTTP.
 * 
 * @author Jeff Nelson
 */
public class HttpLoginTest extends HttpTest {

    @Test
    public void testGetRoot() {
        login();
        List<Long> r1 = bodyAsJava(get("/"), new TypeToken<List<Long>>() {});
        Assert.assertTrue(r1.isEmpty());
        long record = client.add("foo", "bar");
        List<Long> r2 = bodyAsJava(get("/"), new TypeToken<List<Long>>() {});
        Assert.assertTrue(r2.contains(record));
    }

    @Test
    public void testLogin() {
        Response resp = login();
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) bodyAsJson(resp);
        Assert.assertEquals("default", body.get("environment").getAsString());
    }

    @Test
    public void testLoginNonDefaultEnvironment() {
        String environment = TestData.getStringNoDigits().replaceAll(" ", "");
        Response resp = login(environment);
        Assert.assertEquals(200, resp.code());
        JsonObject body = (JsonObject) bodyAsJson(resp);
        Assert.assertEquals(environment, body.get("environment").getAsString());
    }

    @Test
    public void testLoginWithMalformedCredSyntaxThrowsAppropriateError() {
        Response resp = post("/login", "{'user': 'admin', 'pass': 'admin'}");
        JsonObject body = (JsonObject) bodyAsJson(resp);
        Assert.assertEquals(resp.code(), BadLoginSyntaxError.INSTANCE.getCode());
        Assert.assertEquals(body.get("error").getAsString(),
                BadLoginSyntaxError.INSTANCE.getMessage());
    }

    @Test
    public void testLoginAndUseAuthTokenHeader() {
        Response resp = login();
        JsonObject body = (JsonObject) bodyAsJson(resp);
        String token = body.get("token").getAsString();
        clearClientCookies();
        Headers headers = new Headers.Builder().add(
                GlobalState.HTTP_AUTH_TOKEN_HEADER, token).build();
        resp = get("/", headers);
        Assert.assertEquals(200, resp.code());
    }
}
