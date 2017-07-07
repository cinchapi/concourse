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

import java.io.File;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.NaturalSorter;

/**
 * Unit tests for {@link NaturalSorter}.
 * 
 * @author Jeff Nelson
 */
public class NaturalSorterTest {

    private File f1;
    private File f2;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void finished(Description description) {
            f1.delete();
            f2.delete();
        }

    };

    @Test
    public void testDiffTimestamp() {
        f1 = getFile("a");
        f2 = getFile("a");
        Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) < 0);
    }

    @Test
    public void testSameTimestampSameExt() {
        String ts = getTimeString();
        f1 = new File(ts + ".a");
        f2 = new File(ts + ".a");
        Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) == 0);
    }

    @Test
    public void testSameTimestampDiffExt() {
        String ts = getTimeString();
        f1 = new File(ts + ".b");
        f2 = new File(ts + ".a");
        Assert.assertTrue(NaturalSorter.INSTANCE.compare(f1, f2) > 0);
    }

    /**
     * Return a File that is named after the current time string with extension
     * {@code ext}.
     * 
     * @param ext
     * @return a new File
     */
    private File getFile(String ext) {
        return new File(getTimeString() + "." + ext);
    }

    /**
     * Return a string version of the current timestamp.
     * 
     * @return the time string
     */
    private String getTimeString() {
        return Long.toString(Time.now());
    }

}
