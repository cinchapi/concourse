/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.TestData;

/**
 * Test to make sure that {@link Operator#LINKS_TO} works.
 * 
 * @author Jeff Nelson
 */
public class LinksToTest extends ConcourseIntegrationTest {
    
    @Test
    public void testNoLinksButLong(){
        long value = TestData.getLong();
        client.add("foo", value, 1);
        Assert.assertFalse(client.find("foo", Operator.LINKS_TO, value).contains(1L));    
    }
    
    @Test
    public void testLinkAndLong(){
        long value = TestData.getLong();
        while(value == 1){
            value = TestData.getLong();
        }
        client.add("foo", value, 1);
        client.link("foo", value, 1);
        Assert.assertTrue(client.find("foo", Operator.LINKS_TO, value).contains(1L));  
    }
    
    @Test
    public void testLinkAndNoLong(){
        long value = TestData.getLong();
        client.link("foo", value, 1);
        Assert.assertTrue(client.find("foo", Operator.LINKS_TO, value).contains(1L));
    }

}
