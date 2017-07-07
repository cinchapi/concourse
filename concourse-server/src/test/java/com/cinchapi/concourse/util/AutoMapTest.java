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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.AutoMap;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Function;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author Jeff Nelson
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
