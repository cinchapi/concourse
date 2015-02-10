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
package org.cinchapi.concourse;

import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests to show that transactions in Concourse meet the standard for
 * serializable isolation. These examples are inspired by the Wikipedia entry on
 * transaction isolation levels at
 * http://en.wikipedia.org/wiki/Isolation_%28database_systems%29
 * 
 * @author jnelson
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
        client.browse(1);
        client2.add("age", 100, 1);
        System.out.println(client.browse(1));
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
