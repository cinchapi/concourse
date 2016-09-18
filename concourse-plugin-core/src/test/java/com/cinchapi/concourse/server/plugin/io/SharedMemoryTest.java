/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin.io;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Reflection;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link SharedMemory} class.
 * 
 * @author Jeff Nelson
 */
public class SharedMemoryTest {
    
    
    @Test
    public void testBasicWrite(){
        SharedMemory queue = new SharedMemory();
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);        
    }
    
    @Test
    public void testBasicRead(){
        String location = FileOps.tempFile();
        SharedMemory queue = new SharedMemory(location);
        String expected = Random.getString();
        queue.write(ByteBuffers.fromString(expected));
        SharedMemory queue2 = new SharedMemory(location);
        Assert.assertNotEquals(queue, queue2);
        String actual = ByteBuffers.getString(queue.read());
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testCompaction(){
        int toRead = Random.getScaleCount();
        int total = toRead + Random.getScaleCount();
        SharedMemory memory = new SharedMemory();
        List<String> expected = Lists.newArrayList();
        for(int i = 0; i < total; ++i){
            String message = Random.getString();
            memory.write(ByteBuffers.fromString(message));
            expected.add(message);
        }
        List<String> actual = Lists.newArrayListWithExpectedSize(expected.size());
        for(int i = 0; i < toRead; ++i){
            String message = ByteBuffers.getString(memory.read());
            actual.add(message);
        }
        int pos0 = ((StoredInteger) Reflection.get("nextRead", memory)).get();
        memory.compact();
        int pos1 = ((StoredInteger) Reflection.get("nextRead", memory)).get();
        Assert.assertTrue(pos0 > pos1);
        for(int i = toRead; i < total; ++i){
            String message = ByteBuffers.getString(memory.read());
            actual.add(message);
        }
        Assert.assertEquals(expected, actual);
    }

}
