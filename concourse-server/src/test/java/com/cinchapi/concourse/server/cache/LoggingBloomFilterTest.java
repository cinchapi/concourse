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
package com.cinchapi.concourse.server.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.cache.LoggingBloomFilter;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link LoggingBloomFilter}.
 * 
 * @author Jeff Nelson
 */
public class LoggingBloomFilterTest extends ConcourseBaseTest {

    private double fpp = 0.03; // this is the same as the default guava fpp

    private LoggingBloomFilter filter;
    private String file;

    @Override
    protected void beforeEachTest() {
        super.beforeEachTest();
        file = TestData.getTemporaryTestFile();
        filter = LoggingBloomFilter.create(file, 100, fpp);
    }

    @Override
    protected void afterEachTest() {
        super.afterEachTest();
        filter = null;
        FileSystem.deleteFile(file);
    }

    @Test
    public void testDiskSync() {
        Byteable[] byteables = getByteables();
        filter.put(byteables);
        Assert.assertTrue(filter.mightContain(byteables));
        filter.diskSync();
        LoggingBloomFilter filter2 = LoggingBloomFilter.create(file, 100, fpp);
        Assert.assertTrue(filter2.mightContain(byteables));

    }

    @Test
    public void testSameResultAsGuavaBloomFilter() {
        BloomFilter guava = BloomFilter.create(100);
        Byteable[] byteables1 = getByteables();
        Byteable[] byteables2 = getByteables();
        filter.put(byteables1);
        guava.put(byteables1);
        Assert.assertEquals(filter.mightContain(byteables1),
                guava.mightContain(byteables1));
        Assert.assertEquals(filter.mightContain(byteables2),
                guava.mightContain(byteables2));
    }

    @Test
    @Ignore
    public void profileInsertionSpeed() {
        int insertions = 500; // any more insertions and the logging buffer will
                              // overflow :-/
        filter = LoggingBloomFilter.create(file, insertions, fpp);
        BloomFilter guava = BloomFilter.create(insertions);
        List<Byteable[]> byteables = Lists.newArrayList();
        for (int i = 0; i < insertions; i++) {
            byteables.add(getByteables());
        }

        Stopwatch watch = Stopwatch.createUnstarted();
        // Profile Guava
        watch.start();
        for (Byteable[] array : byteables) {
            guava.put(array);
        }
        watch.stop();
        long guavaMs = Variables.register("guava",
                watch.elapsed(TimeUnit.MILLISECONDS));

        // Profile Logging
        watch.reset();
        watch.start();
        for (Byteable[] array : byteables) {
            filter.put(array);
        }
        watch.stop();
        long loggingMs = Variables.register("logging",
                watch.elapsed(TimeUnit.MILLISECONDS));

        long diff = loggingMs - guavaMs;
        if(diff > 0 && diff / guavaMs >= 0.1) { // TODO watch out for division
                                                // by zero if guavaMs is 0
            Assert.fail("Inserting into the logging bloom filter saw a more than 10% slowdown");
        }
        else {
            Assert.assertTrue(true);
        }
    }

    /**
     * Get an array of byteables to put into the bloom filter
     * 
     * @return the byteables
     */
    private Byteable[] getByteables() {
        return new Byteable[] { TestData.getText(), TestData.getValue(),
                TestData.getPrimaryKey() };
    }
}
