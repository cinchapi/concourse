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
package org.cinchapi.concourse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests to to check to make sure that the necessary changes have been made
 * to the codebase after regenerating the thrift code.
 * 
 * @author jnelson
 */
public class ThriftComplianceTest extends ConcourseIntegrationTest {

    @Test
    public void testSwitchedHashToLinkedHash() {
        // This test checks to make sure that all instances of Hash* have been
        // replaced with LinkedHash* in ConcourseService.java
        client.add("name", "john", 1);
        client.add("name", "google", 1);
        client.add("name", "brad", 1);
        client.add("name", "kenneth", 1);
        client.add("name", 1, 1);
        client.add("name", true, 1);
        Timestamp previous = null;
        for (Timestamp timestamp : client.audit(1).keySet()) {
            if(previous != null) {
                Assert.assertTrue(timestamp.getMicros() > previous.getMicros());
            }
            previous = timestamp;
        }
    }

}
