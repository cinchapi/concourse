/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.bugrepro;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.util.StandardActions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test to ensure that we do not encounter a
 * ConcurrentModificationException when expanding the Buffer during a read.
 * 
 * @author jnelson
 */
public class CON75 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean caughtException = new AtomicBoolean(false);
        int count = 0;
        for (int i = 0; i < 247; i++) {
            client.add("__table__", "youtube", i);
            client.add("count", i, i);
            count = i;
        }
        final int value = count;
        final Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        final Concourse client3 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                int count = value;
                while (running.get()) {
                    try {
                        client2.add("__table__", "youtube", count);
                        client2.add("count", count, count);
                        count++;
                    }
                    catch (RuntimeException e) {
                        caughtException.set(true);
                        running.set(false);
                    }
                }

            }

        });

        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                while (running.get()) {
                    try {
                        client3.clear("count",
                                client.search("__table__", "youtube"));
                    }
                    catch (RuntimeException e) {
                        caughtException.set(true);
                        running.set(false);
                    }
                }
            }

        });
        a.start();
        b.start();
        StandardActions.wait(1500, TimeUnit.MILLISECONDS);
        running.set(false);
        Assert.assertFalse(caughtException.get());
    }

}
