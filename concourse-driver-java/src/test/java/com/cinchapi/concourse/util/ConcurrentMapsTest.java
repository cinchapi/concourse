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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

/**
 * Unit tests for {@link ConcurrentMaps}. util class
 * 
 * @author Jeff Nelson
 */
public class ConcurrentMapsTest {

    @Test
    public void testWaitAndRemove() {
        final ConcurrentMap<AccessToken, String> map = Maps.newConcurrentMap();
        String username = Random.getString();
        long salt = Random.getLong();
        long timestamp = Time.now();
        final AccessToken token0 = createAccessToken(username, salt, timestamp);
        final AtomicReference<String> actual = new AtomicReference<String>(null);
        final AtomicBoolean done = new AtomicBoolean(false);
        Thread waiter = new Thread(new Runnable() {

            @Override
            public void run() {
                actual.set(ConcurrentMaps.waitAndRemove(map, token0));
                done.set(true);
            }

        });
        waiter.start();
        AccessToken token = createAccessToken(username, salt, timestamp);
        Assert.assertNotSame(token0, token);
        String expected = Random.getString();
        ConcurrentMaps.putAndSignal(map, token, expected);
        while (!done.get()) {
            continue;
        }
        Assert.assertEquals(expected, actual.get());
    }

    @Test
    public void testWaitAndRemoveAfterDelay() throws InterruptedException {
        int sleep = ConcurrentMaps.SPIN_THRESHOLD_IN_MILLIS;
        ConcurrentMaps.SPIN_THRESHOLD_IN_MILLIS = 100;
        try {
            final ConcurrentMap<AccessToken, String> map = Maps
                    .newConcurrentMap();
            String username = Random.getString();
            long salt = Random.getLong();
            long timestamp = Time.now();
            final AccessToken token0 = createAccessToken(username, salt,
                    timestamp);
            final AtomicReference<String> actual = new AtomicReference<String>(
                    null);
            final AtomicBoolean done = new AtomicBoolean(false);
            Thread waiter = new Thread(new Runnable() {

                @Override
                public void run() {
                    actual.set(ConcurrentMaps.waitAndRemove(map, token0));
                    done.set(true);
                }

            });
            waiter.start();
            AccessToken token = createAccessToken(username, salt, timestamp);
            Assert.assertNotSame(token0, token);
            String expected = Random.getString();
            Thread.sleep(101);
            ConcurrentMaps.putAndSignal(map, token, expected);
            while (!done.get()) {
                continue;
            }
            Assert.assertEquals(expected, actual.get());
        }
        finally {
            ConcurrentMaps.SPIN_THRESHOLD_IN_MILLIS = sleep;
        }
    }

    /**
     * Create an {@link AccessToken} based on all the input components.
     * 
     * @param username
     * @param salt
     * @param timestamp
     * @return a new {@link AccessToken}.
     */
    private static AccessToken createAccessToken(String username, long salt,
            long timestamp) {
        StringBuilder sb = new StringBuilder();
        sb.append(username);
        sb.append(salt);
        sb.append(timestamp);
        AccessToken token = new AccessToken(ByteBuffer.wrap(Hashing.sha256()
                .hashUnencodedChars(sb.toString()).asBytes()));
        return token;
    }

}
