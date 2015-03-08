/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.util.List;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link TArrays} class.
 * 
 * @author jnelson
 */
public class TArraysTest extends ConcourseBaseTest {

    @Test
    public void testConsistentHashing() {
        // Test that the byte buffer that is returned is consistent from JVM
        // session to JVM (e.g. it depends on persistent data).
        List<Integer> bytes = Lists.newArrayList(0,1,-116,-58,-22,32,-65,21,0,1,124,19,90,17,84,-119,0,0,0,1,-55,19,-84,-27);
        Object[] data = { Text.wrap("foo"),
                Value.wrap(Convert.javaToThrift("bar")), PrimaryKey.wrap(1) };
        ByteBuffer buf = TArrays.hash(data);
        for (int i = 0; i < buf.capacity(); i++) {
            Assert.assertEquals(buf.get(), bytes.get(i).byteValue());
        }
    }
    
    @Test
    public void testEqualObjectsHaveEqualHash(){
        Object[] data = {TestData.getText(), TestData.getValue(), TestData.getPrimaryKey()};
        ByteBuffer a = TArrays.hash(data);
        ByteBuffer b = TArrays.hash(data);
        Assert.assertEquals(a, b);
    }
    
}
