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

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.util.StandardActions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Repro of <a href="https://cinchapi.atlassian.net/browse/CON-52">CON-52</a>
 * where search can temporarily return inconsistent results when
 * data is being transported from the buffer to the database.
 * 
 * 
 * @author jnelson
 */
public class CON52 extends ConcourseIntegrationTest {

    @Test
    public void test() {
        StandardActions.import1027YoutubeLinks(client);
        for (int i = 0; i < 20; i++) {
            int size = client.search("Youtube Embed Link", "youtube").size();
            Assert.assertEquals(size, 1027);
            StandardActions.wait(5, TimeUnit.MILLISECONDS); // slight delay to
                                                            // give data time to
                                                            // transport
        }

    }

}
