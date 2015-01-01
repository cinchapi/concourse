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

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.Timestamp;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Repro of issue described in CON-173 where an atomic operation inside of a
 * transaction spins forever if the transaction fails unexpectedly due to a
 * version change.
 * 
 * @author jnelson
 */
public class CON173 extends ConcourseIntegrationTest {

    Concourse client2;

    @Override
    public void beforeEachTest() {
        client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
    }

    @Test(timeout = 1000)
    public void reproChronologize() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.chronologize("foo", 1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproClearKeyRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.clear("foo", 1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproClearRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.clear(1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproFindCriteria() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.find(Criteria.where().key("foo").operator(Operator.EQUALS)
                    .value("baz"));
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproInsert() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.insert("{\"foo\":\"baz\"}");
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproInsertInExistingRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.insert("{\"foo\":\"baz\"}", 1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproVerifyAndSwap() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.verifyAndSwap("foo", "bar", 1, "baz");
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproVerifyOrSet() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.verifyOrSet("foo", "grow", 1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

    @Test(timeout = 1000)
    public void reproRevert() {
        try {
            client.stage();
            client.get("foo", 1);
            Timestamp ts = Timestamp.now();
            client.set("foo", "bar", 1);
            client2.set("foo", "baz", 1);
            client.revert("foo", 1, ts);
            client.get("foo", 1);
            Assert.assertTrue(true); // this means we did not timeout
        }
        catch (Exception e) {
            Assert.assertTrue(true); // An exception means we passed because the
                                     // transaction failure has propagated up.
        }
        finally {
            client.abort();
        }
    }

}
