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
package com.cinchapi.concourse.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.ConcurrentLoadingQueue;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ConcurrentLoadingQueue}.
 * 
 * @author Jeff Nelson
 */
public class ConcurrentLoadingQueueTest {

    @Test
    public void testDynamicLoading() {
        final AtomicInteger counter = new AtomicInteger();
        ConcurrentLoadingQueue<Integer> queue = ConcurrentLoadingQueue
                .create(new Callable<Integer>() {

                    @Override
                    public Integer call() throws Exception {
                        return counter.incrementAndGet();
                    }

                });
        Assert.assertEquals(1, (int) queue.peek());
        Assert.assertEquals(1, (int) queue.peek());
        Assert.assertEquals(1, (int) queue.poll());
        Assert.assertEquals(2, (int) queue.poll());
    }

    @Test
    public void testDynamicLoadingWithInitialValues() {
        int count = Random.getScaleCount();
        final AtomicInteger counter = new AtomicInteger(count - 1);
        List<Integer> initial = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            initial.add(i);
        }
        ConcurrentLoadingQueue<Integer> queue = ConcurrentLoadingQueue.create(
                initial, new Callable<Integer>() {

                    @Override
                    public Integer call() throws Exception {
                        return counter.incrementAndGet();
                    }

                });
        for (int i = 0; i < count * 2; i++) {
            Assert.assertEquals(i, (int) queue.poll());
        }
    }
    
    @Test
    public void testDynamicLoadOnlyWhenNecessary(){
        ConcurrentLoadingQueue<Integer> queue = ConcurrentLoadingQueue.create(new Callable<Integer>(){

            @Override
            public Integer call() throws Exception {
                return 1;
            }
            
        });
        queue.offer(100);
        queue.offer(200);
        Assert.assertEquals(100, (int) queue.peek());
        Assert.assertEquals(100, (int) queue.poll());
        Assert.assertEquals(200, (int) queue.poll());
        Assert.assertEquals(1, (int) queue.poll());
    }

}
