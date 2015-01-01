/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ConcurrentLoadingQueue}.
 * 
 * @author jnelson
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
