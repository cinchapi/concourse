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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the functions in the {@link Queues} class.
 * 
 * @author Jeff Nelson
 */
public class QueuesTest {

    @Test
    public void testBlockingDrain() throws InterruptedException {
        final BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            queue.add(Random.getObject());
        }
        queue.drainTo(Lists.newArrayList());
        List<Object> buffer = Lists.newArrayList();
        final AtomicInteger expected = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                List<Object> objects = Lists.newArrayList();
                for (int i = 0; i < Random.getScaleCount(); ++i) {
                    objects.add(Random.getObject());
                }
                expected.set(objects.size());
                queue.addAll(objects);
                latch.countDown();
            }

        });
        Random.sleep();
        t.start();
        latch.await();
        int actual = Queues.blockingDrain(queue, buffer);
        Assert.assertEquals(expected.get(), actual);
        Assert.assertNull(queue.poll());
    }

}
