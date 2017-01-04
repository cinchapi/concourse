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
package com.cinchapi.concourse.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests to to check to make sure that the necessary changes have been made
 * to the codebase after regenerating the thrift code.
 * 
 * @author Jeff Nelson
 */
public class ThriftComplianceTest extends ConcourseIntegrationTest {

    @Test
    public void testSwitchedHashToLinkedHash() {
        // This test checks to make sure that all instances of Hash* have been
        // replaced with LinkedHash* in ConcourseService.java
        client.add("name", "john", 1);
        client.add("name", "google", 1);
        client.add("name", "brad", 1);
        client.add("name", "kenneth", 1);
        client.add("name", 1, 1);
        client.add("name", true, 1);
        Timestamp previous = null;
        for (Timestamp timestamp : client.audit(1).keySet()) {
            if(previous != null) {
                Assert.assertTrue(timestamp.getMicros() > previous.getMicros());
            }
            previous = timestamp;
        }
    }

    @Test
    public void testGetTObjectInternalType() {
        TObject tObject = new TObject(ByteBuffer.wrap(Random.getString()
                .getBytes()), Type.TAG);
        Assert.assertEquals(Type.STRING, tObject.getInternalType());
    }

    @Test
    public void testTObjectHashCode() {
        TObject tObject = new TObject(ByteBuffer.wrap(Random.getString()
                .getBytes()), Type.TAG);
        Assert.assertEquals(
                tObject.hashCode(),
                Arrays.hashCode(new int[] { tObject.data.hashCode(),
                        tObject.getInternalType().ordinal() }));
    }
    
    @Test
    public void testTObjectToString(){
        Object object = TestData.getObject();
        TObject tObject = Convert.javaToThrift(object);
        Assert.assertEquals(object.toString(), tObject.toString());
    }

}
