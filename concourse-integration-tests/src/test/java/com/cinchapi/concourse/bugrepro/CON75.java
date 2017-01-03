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
package com.cinchapi.concourse.bugrepro;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.StandardActions;

/**
 * Unit test to ensure that we do not encounter a
 * ConcurrentModificationException when expanding the Buffer during a read.
 * 
 * @author Jeff Nelson
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
