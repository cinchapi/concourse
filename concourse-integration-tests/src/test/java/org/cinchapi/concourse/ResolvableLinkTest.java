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

import java.util.Set;

import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

/**
 * Integration tests to verify the resolvable link functionality.
 * 
 * @author Jeff Nelson
 */
public class ResolvableLinkTest extends ConcourseIntegrationTest {
    
    @Test
    public void testInsertRemoteResolvableLink(){
        String key = "age";
        int value = 10;
        int count = TestData.getScaleCount();
        for(int i = 1; i <= value + count; ++i){
            client.add(key, i);
        }
        JsonObject json = new JsonObject();
        json.addProperty("friends", Convert.stringToResolvableLinkInstruction("age > "+value));
        client.insert(json.toString(), 1);
        Set<Object> friends = client.select("friends", 1);
        Assert.assertEquals(count, friends.size());
    }

}
