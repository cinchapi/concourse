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

/**
 * Unit tests to show that transactions in Concourse meet the standard for
 * serializable isolation. These examples are inspired by the Wikipedia entry on
 * transaction isolation levels at
 * http://en.wikipedia.org/wiki/Isolation_%28database_systems%29
 * 
 * @author Jeff Nelson
 */
public class TransactionIsolationTest extends ConcourseIntegrationTest {

    private Concourse client2;

    @Override
    protected void beforeEachTest() {
        client2 = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
    }

    @Test
    public void testNoDirtyRead() {
        client.stage();
        client2.stage();
        client.add("foo", "bar", 1);
        Assert.assertNotEquals("bar", client2.get("foo", 1));
    }

    @Test(expected = TransactionException.class)
    public void testNoNonRepeatableRead() {
        client.add("name", "Jeff Nelson", 1);
        client.stage();
        client.select(1);
        client2.add("age", 100, 1);
        System.out.println(client.select(1));
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomRead() {
        client.add("foo", 10, 1);
        client.stage();
        client.find(Criteria.where().key("foo").operator(Operator.BETWEEN)
                .value(5).value(20));
        client2.add("foo", 15, 2);
        client.find(Criteria.where().key("foo").operator(Operator.BETWEEN)
                .value(5).value(20));
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomReadWithTimeStampInTheFutureUsingBrowse() {
        Timestamp aheadOfTime = Timestamp.fromMicros(Time.now() + (long) 10e9);
        client.add("foo", "bar", 100);
        client.stage();
        client.browse("foo", aheadOfTime);
        client2.add("foo", "bar", 100 + 1);
        client.commit();
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomReadWithTimeStampInTheFutureUsingDescribe() {
        Timestamp aheadOfTime = Timestamp.fromMicros(Time.now() + (long) 10e9);
        client.add("foo", "bar", 110);
        client.stage();
        client.describe(110, aheadOfTime);
        client2.add("bar", "foo", 110);
        client.commit();
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomReadWithTimeStampInTheFutureUsingFind() {
        Timestamp aheadOfTime = Timestamp.fromMicros(Time.now() + (long) 10e9);
        client.add("foo", 50, 120);
        client.stage();
        client.find("foo", Operator.BETWEEN, 0, 100, aheadOfTime);
        client2.add("foo", 75, 120);
        client.commit();
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomReadWithTimeStampInTheFutureUsingGet() {
        Timestamp aheadOfTime = Timestamp.fromMicros(Time.now() + (long) 10e9);
        client.add("foo", "bar", 130);
        client.stage();
        client.get("foo", 130, aheadOfTime);
        client2.add("foo", "foobar", 130);
        client.commit();
    }

    @Test(expected = TransactionException.class)
    public void testNoPhantomReadWithTimeStampInTheFutureUsingVerify() {
        Timestamp aheadOfTime = Timestamp.fromMicros(Time.now() + (long) 10e9);
        client.add("foo", "bar", 140);
        client.stage();
        client.verify("foo", "bar", 140, aheadOfTime);
        client2.add("foo", "foobar", 140);
        client.commit();
    }

    @Test
    public void testNoWriteSkew() {
        client.set("balance", 100, 1);
        client.set("balance", 100, 2);
        client.stage();
        client2.stage();
        final AtomicBoolean done1 = new AtomicBoolean(false);
        final AtomicBoolean done2 = new AtomicBoolean(false);
        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    client.stage();
                    client.set("balance",
                            ((int) (client.get("balance", 1))) - 200, 1);
                    if((int) client.get("balance", 1)
                            + (int) client.get("balance", 2) >= 0) {
                        client.commit();
                    }
                }
                catch (TransactionException e) {
                    client.abort();
                }
                finally {
                    done1.set(true);
                }

            }

        });

        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    client2.stage();
                    client2.set("balance",
                            ((int) (client2.get("balance", 1))) - 200, 1);
                    if((int) client2.get("balance", 1)
                            + (int) client2.get("balance", 2) >= 0) {
                        client2.commit();
                    }
                }
                catch (TransactionException e) {
                    client2.abort();
                }
                finally {
                    done2.set(true);
                }

            }

        });
        t1.start();
        t2.start();
        while (!done1.get() || !done2.get()) {
            continue;
        }
        Assert.assertEquals(0,
                (int) client.get("balance", 1) + (int) client.get("balance", 2));
    }

}
