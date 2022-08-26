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
package com.cinchapi.concourse.collect;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SharedHashSet}.
 *
 * @author Jeff Nelson
 */
public class ShardedHashSetTest {

    @Test
    public void testIterator() {
        Set<Integer> expected = new HashSet<>();
        Set<Integer> data = new ShardedHashSet<>();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            int value = TestData.getInt();
            expected.add(value);
            data.add(value);
        }
        Iterator<Integer> it = data.iterator();
        try {
            Set<Integer> actual = new LinkedHashSet<>();
            while (it.hasNext()) {
                actual.add(it.next());
            }
            Assert.assertEquals(expected, actual);
        }
        finally {
            Iterators.close(it);
        }
    }

    @Test
    public void testConcurrentIteratorReadOnly() throws InterruptedException {
        Set<Integer> expected = new HashSet<>();
        ShardedHashSet<Integer> data = new ShardedHashSet<>();
        for (int i = 0; i < TestData.getScaleCount() * 5; ++i) {
            int value = TestData.getInt();
            expected.add(value);
            data.add(value);
        }
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Thread t = new Thread(() -> {
                Set<Integer> actual = new LinkedHashSet<>();
                CloseableIterator<Integer> it = data.concurrentIterator();
                try {
                    while (it.hasNext()) {
                        actual.add(it.next());
                    }
                    Assert.assertEquals(expected, actual);
                }
                finally {
                    Iterators.close(it);
                }
            });
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void testConcurrentIteratorWithAdds() throws InterruptedException {
        Set<Integer> expected = new HashSet<>();
        ShardedHashSet<Integer> data = new ShardedHashSet<>();
        for (int i = 0; i < TestData.getScaleCount() * 5; ++i) {
            int value = TestData.getInt();
            expected.add(value);
            data.add(value);
        }
        CountUpLatch latch = new CountUpLatch();
        List<Thread> threads = new ArrayList<>();
        AtomicBoolean done = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            while (!done.get()) {
                data.add(TestData.getInt());
            }
        });
        thread.setDaemon(true);
        thread.start();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            Thread t = new Thread(() -> {
                Set<Integer> actual = new LinkedHashSet<>();
                CloseableIterator<Integer> it = data.concurrentIterator();
                try {
                    while (it.hasNext()) {
                        actual.add(it.next());
                    }
                    for (int value : expected) {
                        Assert.assertTrue(actual.contains(value));
                    }
                    latch.countUp();
                }
                finally {
                    Iterators.close(it);
                }
            });
            t.start();
            threads.add(t);
        }
        latch.await(threads.size());
        for (Thread t : threads) {
            t.join();
        }
        done.set(true);
        thread.join();
    }

    @Test
    public void testEquals() {
        Set<Integer> expected = new HashSet<>();
        Set<Integer> data = new ShardedHashSet<>();
        for (int i = 0; i < TestData.getScaleCount(); ++i) {
            int value = TestData.getInt();
            expected.add(value);
            data.add(value);
        }
        Assert.assertEquals(expected, data);
    }

    @Test
    public void testSize() {
        // Based on
        // https://www.cs.cornell.edu/courses/cs2110/2016sp/recitations/recitation07/HashSetTester.java
        Set<String> set = new ShardedHashSet<>();
        Assert.assertTrue(set.add("abc"));
        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains("abc"));
        Assert.assertFalse(set.contains("abcd"));

        Assert.assertFalse(set.add("abc"));
        Assert.assertEquals(1, set.size());
        Assert.assertTrue(set.contains("abc"));
        Assert.assertFalse(set.contains("abcd"));

        set.add("abcd");
        Assert.assertEquals(2, set.size());
        Assert.assertTrue(set.contains("abc"));
        Assert.assertTrue(set.contains("abcd"));
    }

}
