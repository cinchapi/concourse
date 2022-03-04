/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.temp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.storage.CommitVersions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * Unit tests for {@link ToggleQueue}.
 *
 * @author Jeff Nelson
 */
public class ToggleQueueTest extends QueueTest {

    @Override
    protected Queue getStore() {
        return new ToggleQueue(100);
    }

    @Test
    public void testToggleWriteSanityCheck() {
        Write add = Write.add("name", Convert.javaToThrift("jeff"), 1);
        Write remove = Write.remove("name", Convert.javaToThrift("jeff"), 1);
        ToggleQueue queue = (ToggleQueue) store;
        long version = CommitVersions.next();
        queue.insert(remove.rewrite(version));
        Assert.assertEquals(1, Iterators.size(queue.iterator()));
        queue.insert(add.rewrite(version));
        Assert.assertEquals(0, Iterators.size(queue.iterator()));
        queue.insert(remove);
        Assert.assertEquals(1, Iterators.size(queue.iterator()));
        queue.insert(add);
        Assert.assertEquals(2, Iterators.size(queue.iterator()));
    }

    @Test
    public void testToggleWrites() {
        Map<Write, List<Write>> writes = new HashMap<>();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Write write = TestData.getScaleCount() % 2 == 0
                    ? TestData.getWriteAdd()
                    : TestData.getWriteRemove();
            writes.put(write, new ArrayList<>());
        }
        ToggleQueue queue = (ToggleQueue) store;
        Random rand = new Random();
        long version = CommitVersions.next();
        for (int i = 0; i < writes.size() * 2.5; ++i) {
            int index = rand.nextInt(writes.size());
            Write write = Iterables.get(writes.keySet(), index);
            write = Iterables.getLast(writes.get(write), write);
            write = write.inverse().rewrite(version);
            queue.insert(write);
            writes.get(write).add(write);
        }
        int expected = 0;
        for (List<Write> value : writes.values()) {
            if(Numbers.isEven(value.size())) {
                expected += 0;
            }
            else {
                expected += 1;
            }
        }
        System.out.println(expected);
        Assert.assertEquals(expected, queue.size());
    }

}
