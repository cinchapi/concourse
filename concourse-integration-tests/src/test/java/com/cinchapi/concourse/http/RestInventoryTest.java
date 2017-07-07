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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Response;

/**
 * Unit tests for the #inventory() functionality of Concourse as exposed by the
 * REST API.
 * 
 * @author Jeff Nelson
 */
public class RestInventoryTest extends RestTest {

    @Test
    public void testInventoryNonDefaultEnvironment() {
        String env = Random.getSimpleString();
        client = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin",
                env);
        long record = client.add("name", "jeff");
        login(env);
        Response response = get("/");
        Set<Long> body = bodyAsJava(response, new TypeToken<Set<Long>>() {});
        Assert.assertEquals(Sets.newHashSet(record), body);
    }
    
    @Test
    public void testInventoryDefaultEnvironment(){
        long record = client.add("name", "jeff");
        Response response = get("/");
        Set<Long> body = bodyAsJava(response, new TypeToken<Set<Long>>() {});
        Assert.assertEquals(Sets.newHashSet(record), body);
    }

}
