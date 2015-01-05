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
package org.cinchapi.concourse.server.cache;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.cache.BloomFilter;
import org.cinchapi.concourse.server.storage.cache.LoggingBloomFilter;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link LoggingBloomFilter}.
 * 
 * @author jnelson
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
