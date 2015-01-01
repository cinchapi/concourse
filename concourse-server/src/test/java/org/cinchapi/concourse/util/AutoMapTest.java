/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.testing.Variables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Function;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class AutoMapTest extends ConcourseBaseTest {

    private static final long CLEANUP_DELAY = 500;
    private static final TimeUnit CLEANUP_DELAY_UNIT = TimeUnit.MILLISECONDS;

    @ClassRule
    public static TestRule WATCHER = new TestWatcher() {

        @Override
        protected void starting(Description desc) {
            AutoMap.setCleanupDelay(CLEANUP_DELAY, CLEANUP_DELAY_UNIT);
        }
    };

    private static final Function<String, Set<String>> LOADER = new Function<String, Set<String>>() {

        @Override
        @Nullable
        public Set<String> apply(@Nullable String input) {
            return Sets.newHashSet();
        }

    };

    private static final Function<Set<String>, Boolean> CLEANER = new Function<Set<String>, Boolean>() {

        @Override
        @Nullable
        public Boolean apply(@Nullable Set<String> input) {
            return input.isEmpty();
        }

    };

    private AutoMap<String, Set<String>> instance;

    @Rule
    public TestRule rule = new TestWatcher() {

        @Override
        protected void starting(Description desc) {
            instance = Variables.register("instance", getInstance(LOADER, CLEANER));
        }
    };

    @Test
    public void testLoader() {
        String key = TestData.getString();
        Assert.assertFalse(instance.containsKey(key));
        Assert.assertNotNull(instance.get(key));
        Assert.assertTrue(instance.containsKey(key));
    }

    @Test
    public void testCleanupIfEmpty() throws InterruptedException {
        String key = TestData.getString();
        instance.get(key);
        Assert.assertTrue(instance.containsKey(key));
        CLEANUP_DELAY_UNIT.sleep(CLEANUP_DELAY * 2); // allow buffer in case
                                                     // cleanup can't run as
                                                     // scheduled
        Assert.assertFalse(instance.containsKey(key));
    }

    @Test
    public void testCleanupIfNotEmpty() throws InterruptedException {
        String key = TestData.getString();
        instance.get(key).add(TestData.getString());
        CLEANUP_DELAY_UNIT.sleep(CLEANUP_DELAY * 2); // allow buffer in case
                                                     // cleanup can't run as
                                                     // scheduled
        Assert.assertTrue(instance.containsKey(key));
    }

    protected abstract AutoMap<String, Set<String>> getInstance(
            Function<String, Set<String>> loader,
            Function<Set<String>, Boolean> cleaner);

}
