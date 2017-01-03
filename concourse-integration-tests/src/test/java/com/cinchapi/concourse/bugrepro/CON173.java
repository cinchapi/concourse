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
package com.cinchapi.concourse.bugrepro;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Repro of issue described in CON-173 where an atomic operation inside of a
 * transaction spins forever if the transaction fails unexpectedly due to a
 * version change.
 * 
 * @author Jeff Nelson
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
            Assert.assertFalse(client.verifyAndSwap("foo", "bar", 1, "baz"));
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
