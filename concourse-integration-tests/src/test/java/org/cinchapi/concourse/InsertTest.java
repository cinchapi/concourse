/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

/**
 * Unit tests for the {@code insert()} API methods
 * 
 * @author Jeff Nelson
 */
public class InsertTest extends ConcourseIntegrationTest {

    @Test
    public void testInsertInteger() {
        JsonObject object = new JsonObject();
        String key = "foo";
        int value = 1;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertTagStripsBackticks() { // CON-157
        JsonObject object = new JsonObject();
        String key = "__section__";
        String value = "org.cinchapi.concourse.oop.Person";
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBoolean() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBooleanAsTag() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertEquals(Boolean.toString(value), client.get(key, record));
    }

    @Test
    public void testInsertResolvableLink() {
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse",
                Convert.stringToResolvableLinkSpecification("name", "Jeff"));
        String json = object.toString();
        client.insert(json, 2);
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(
                2L));
    }

    @Test
    public void testInsertResolvableLinkIntoNewRecord() {
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse",
                Convert.stringToResolvableLinkSpecification("name", "Jeff"));
        String json = object.toString();
        long record = client.insert(json).iterator().next();
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(
                record));
    }

}
