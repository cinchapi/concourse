/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Strings;

/**
 * ETE tests for transaction related workflows.
 * 
 * @author Jeff Nelson
 */
public class TransactionWorkflowTest extends ConcourseIntegrationTest {

    /**
     * A second client that is connected to the server on behalf a different,
     * non-admin user.
     */
    private Concourse client2;

    @Override
    protected void beforeEachTest() {
        String username = null;
        while (Strings.isNullOrEmpty(username)) {
            username = TestData.getSimpleString();
        }
        String password = TestData.getString();
        while (Strings.isNullOrEmpty(password) || password.length() < 3) {
            password = TestData.getString();
        }
        grantAccess(username, password);
        client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, username,
                password);
    }

    @Test
    public void testIsolation() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.stage();
        client2.stage();
        client.add(key, value, record);
        Assert.assertTrue(client.verify(key, value, record));
        Assert.assertFalse(client2.verify(key, value, record));
    }

    @Test
    public void testDurabilityAfterServerRestart() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = TestData.getLong();
        client.stage();
        client.add(key, value, record);
        client.commit();
        restartServer();
        Assert.assertTrue(client.verify(key, value, record));
    }

    @Test
    public void testConcurrentTransactionsForSameUserAreCorrectlyRouted() {
        client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
        client.stage();
        client2.stage();
        client.add("foo", "bar", 1);
        client2.add("foo", "bar", 2);
        Assert.assertTrue(client.verify("foo", "bar", 1));
        Assert.assertFalse(client.verify("foo", "bar", 2));
        Assert.assertTrue(client2.verify("foo", "bar", 2));
        Assert.assertFalse(client2.verify("foo", "bar", 1));
    }

    @Test
    public void testConcurrentTransactionsForSameUserCanBeCommitted() {
        client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
        client.stage();
        client2.stage();
        client.add("foo", "bar", 1);
        client2.add("foo", "bar", 2);
        Assert.assertTrue(client.commit());
        Assert.assertTrue(client2.commit());
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionGet() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.get("foo", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionRevert() {
        try {
            client.stage();
            client.get("foo", 1);
            Timestamp ts = Timestamp.now();
            client.set("foo", "bar", 1);
            client2.set("foo", "baz", 1);
            client.revert("foo", 1, ts);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionAdd() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.add("foo", "grow", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionAudit() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.audit("foo", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionAuditRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.audit(1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionBrowseRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.select(1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionBrowseKey() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.browse("foo");
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionChronologize() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.chronologize("foo", 1);
        }
        finally {
            client.abort();
        }
    }
    
    @Test(expected = TransactionException.class)
    public void testChronologizeWithFutureEndTimestampGrabsLock(){
        try{
            client.stage();
            client.chronologize("foo", 1, Timestamp.epoch(), Timestamp.fromMicros(Time.now() + 100000000));
            client2.set("foo", "baz", 1);
            Assert.assertFalse(client.commit());
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionClear() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.clear("foo", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionClearRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.clear(1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionCommit() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.commit();
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionDescribeRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.describe(1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionDescribeKeyFetch() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.select("foo", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionFind() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.find("foo", Operator.EQUALS, "bar");
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionFindCriteria() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.find(Criteria.where().key("foo").operator(Operator.EQUALS)
                    .value("bar"));
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionInsert() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.insert("{\"foo\": \"bar\"}", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionInsertNewRecord() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.insert("{\"foo\": \"bar\"}");
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionPing() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.ping(1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionRemove() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.remove("foo", "baz", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionSearch() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.search("foo", "bar");
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionSet() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.set("foo", "bar", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionVerify() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.verify("foo", "bar", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionVerifyAndSwap() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.verifyAndSwap("foo", "bar", 1, "baz");
        }
        finally {
            client.abort();
        }
    }

    @Test(expected = TransactionException.class)
    public void testPreCommitTransactionFailuresAreIndicatedWithExceptionVerifyOrSet() {
        try {
            client.stage();
            client.get("foo", 1);
            client2.set("foo", "baz", 1);
            client.verifyOrSet("foo", "bar", 1);
        }
        finally {
            client.abort();
        }
    }

    @Test
    public void testCommitWhenNotInTransactionReturnsFalse() {
        Assert.assertFalse(client.commit());
    }

    @Test
    public void testStageRunnableCommit() {

        boolean committed = client.stage(new Runnable() {

            @Override
            public void run() {
                client.add("name", "Ron", 1);
                client.add("name", "Stacy", 2);

            }

        });
        Assert.assertTrue(committed);
        Assert.assertEquals("Ron", client.get("name", 1));
        Assert.assertEquals("Stacy", client.get("name", 2));
        Assert.assertEquals("Ron", client2.get("name", 1));
        Assert.assertEquals("Stacy", client2.get("name", 2));

    }

    @Test
    public void testStageRunnableFailsOnConfilct() throws InterruptedException {

        final AtomicBoolean t1Go = new AtomicBoolean(false);
        final AtomicBoolean t2Go = new AtomicBoolean(false);
        final AtomicBoolean committed = new AtomicBoolean(true);
        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    committed.set(client.stage(new Runnable() {
                        public void run() {
                            client.add("name", "Ron", 1);
                            t2Go.set(true);
                            while (!t1Go.get()) {
                                continue;
                            }
                        }

                    }));
                }
                catch (TransactionException e) {
                    committed.set(false);
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!t2Go.get()) {
                    continue;
                }
                client2.add("name", "Bron", 1);
                t1Go.set(true);
            }

        });
        t1.start();
        t2.start();
        t2.join();
        t1.join();
        Assert.assertFalse(committed.get());
        Assert.assertFalse(client.select("name", 1).contains("Ron"));
    }

}
