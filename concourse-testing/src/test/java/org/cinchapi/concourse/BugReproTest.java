/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * Any bugs that are found/filed should be reproduced in this class prior to
 * making a code change to fix the issue. The repro test should fail prior to
 * the fix and should pass afterwards.
 * 
 * @author jnelson
 */
public class BugReproTest extends ConcourseIntegrationTest {

    @Test
    public void testCON_4() {
        StandardActions.importWordsDotText(client);
        StandardActions.wait(75, TimeUnit.SECONDS);
        StandardActions.import1000Longs(client);
        StandardActions.wait(10, TimeUnit.SECONDS);
        Assert.assertTrue(client.describe(1).contains("count"));
        client.add("count", 2, 1);
        Assert.assertTrue(client.describe(1).contains("count"));
    }

    @Test
    public void testCON_5() {
        StandardActions.importWordsDotText(client);
        StandardActions.wait(75, TimeUnit.SECONDS);
        StandardActions.import1000Longs(client);
        StandardActions.wait(10, TimeUnit.SECONDS);
        client.add("count", 2, 1);
        client.set("count", 10, 1);
        client.remove("count", 10, 1);
        Assert.assertFalse(client.describe(1).contains("count"));
    }

}
