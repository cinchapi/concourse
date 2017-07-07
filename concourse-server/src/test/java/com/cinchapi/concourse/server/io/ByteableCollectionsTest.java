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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link ByteableCollections} util class.
 * 
 * @author Jeff Nelson
 */
public class ByteableCollectionsTest extends ConcourseBaseTest {

    @Test
    public void testStreamingIterator() {
        String file = TestData.getTemporaryTestFile();
        List<Value> values = Lists.newArrayList();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; ++i) {
            values.add(TestData.getValue());
        }
        ByteBuffer bytes = ByteableCollections.toByteBuffer(values);
        FileSystem.writeBytes(bytes, file);
        int bufferSize = TestData.getScaleCount();
        Iterator<ByteBuffer> it = ByteableCollections.streamingIterator(file,
                bufferSize);
        List<Value> newValues = Lists.newArrayList();
        while (it.hasNext()) {
            newValues.add(Value.fromByteBuffer(it.next()));
        }
        Assert.assertEquals(values, newValues);
    }

}
