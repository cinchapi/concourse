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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.TestData;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Response;

/**
 * Unit tests for performing writes using the REST API.
 * 
 * @author Jeff Nelson
 */
public class RestWriteTest extends RestTest {
    
    @Test
    public void testInsertJsonInRecord(){
        long record = TestData.getLong();
        JsonObject json = new JsonObject();
        json.addProperty("boolean", true);
        json.addProperty("number", TestData.getScaleCount());
        json.addProperty("string", TestData.getSimpleString());
        Response resp = post("/{0}", json.toString(), record);
        boolean body = bodyAsJava(resp, TypeToken.get(Boolean.class));
        Assert.assertTrue(body);
        Assert.assertEquals(200, resp.code());
        Assert.assertEquals(json.get("boolean").getAsBoolean(), client.get("boolean", record));
        Assert.assertEquals(json.get("number").getAsNumber(), client.get("number", record));
        Assert.assertEquals(json.get("string").getAsString(), client.get("string", record));
        
    }
    
    @Test
    public void testInsertKeyAsValueInNewRecord(){
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        String strValue = prepareForJsonImport(value);
        Response resp = post("/{0}", strValue, key);
        long record = bodyAsJava(resp, TypeToken.get(Long.class));
        Assert.assertEquals(200, resp.code());
        Assert.assertEquals(value, client.get(key, record));
    }
    
    

}
