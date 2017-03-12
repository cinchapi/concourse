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
package com.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TArrays;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link TArrays} class.
 * 
 * @author Jeff Nelson
 */
public class TArraysTest extends ConcourseBaseTest {

    @Test
    public void testConsistentHashing() {
        // Test that the byte buffer that is returned is consistent from JVM
        // session to JVM (e.g. it depends on persistent data).
        List<Integer> bytes = Lists.newArrayList(0, 1, -116, -58, -63, -90, 58,
                -104, 0, 67, 101, -107, 115, 59, 73, 102, 0, 0, 0, 1, -39, 33,
                58, 40);
        Object[] data = { Text.wrap("foo"),
                Value.wrap(Convert.javaToThrift("bar")), PrimaryKey.wrap(1) };
        ByteBuffer buf = TArrays.hash(data);
        System.out.println(buf);
        for (int i = 0; i < buf.capacity(); i++) {
            Assert.assertEquals(buf.get(), bytes.get(i).byteValue());
        }
    }

    @Test
    public void testEqualObjectsHaveEqualHash() {
        Object[] data = { TestData.getText(), TestData.getValue(),
                TestData.getPrimaryKey() };
        ByteBuffer a = TArrays.hash(data);
        ByteBuffer b = TArrays.hash(data);
        Assert.assertEquals(a, b);
    }

}
