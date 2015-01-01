/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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
 * @author jnelson
 */
public class InsertTest extends ConcourseIntegrationTest {

    @Test
    public void testInsertInteger() {
        JsonObject object = new JsonObject();
        String key = "foo";
        int value = 1;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json);
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertTagStripsBackticks() { // CON-157
        JsonObject object = new JsonObject();
        String key = "__section__";
        String value = "org.cinchapi.concourse.oop.Person";
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json);
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBoolean() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, value);
        String json = object.toString();
        long record = client.insert(json);
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testInsertBooleanAsTag() {
        JsonObject object = new JsonObject();
        String key = "foo";
        boolean value = false;
        object.addProperty(key, "`" + value + "`");
        String json = object.toString();
        long record = client.insert(json);
        Assert.assertEquals(Boolean.toString(value), client.get(key, record));
    }
    
    @Test
    public void testInsertResolvableLink(){
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse", Convert.stringToResolvableLinkSpecification("name", "Jeff"));
        String json = object.toString();
        client.insert(json, 2);
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(2L));      
    }
    
    @Test
    public void testInsertResolvableLinkIntoNewRecord(){
        client.set("name", "Jeff", 1);
        JsonObject object = new JsonObject();
        object.addProperty("name", "Ashleah");
        object.addProperty("spouse", Convert.stringToResolvableLinkSpecification("name", "Jeff"));
        String json = object.toString();
        long record = client.insert(json);
        Assert.assertTrue(client.find("spouse", Operator.LINKS_TO, 1).contains(record));
    }

}
