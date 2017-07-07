/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.cinchapi.concourse.example.bank;

import java.text.MessageFormat;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.test.Variables;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * This is a test to show that the Concourse backed implementation for the Bank
 * application satisfies the necessary constraints.
 * <p>
 * This test takes advantage of the <em>concourse-ete-test-core</em> framework,
 * which allows us to easily define test cases that run application source code
 * against a real Concourse server instance that is completely managed for us.
 * </p>
 * <p>
 * When using this test framework, we can make assertions about our business
 * logic (like we would do anyway) and simultaneously make direct assertions
 * about what data is stored in the database without doing any extra work.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ConcourseBankTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        // This allows us to control the exact version of Concourse against
        // which the tests will run. This should always match the version of
        // Concourse that is used when the application is deployed (i.e. 0.4.4,
        // 0.5.0, etc). The nice thing about this is that whenever the Concourse
        // version is upgraded for the application, we can simply change the
        // version number used within our tests and ensure that none of the
        // application code has broken as a result of the upgrade.
        return "latest";
    }

    @Override
    public void beforeEachTest() {
        // Here we can define tasks that are run before each test case.
        // Since the application code uses the default connection scheme, we
        // have to configure the tests to use the connection information that
        // maps to the test server provided by the framework.
        server.syncDefaultClientConnectionInfo();
        Constants.refreshConnectionInfo();
    }

    @Override
    public void afterEachTest() {
        // Here we can define tasks that are run after each test case. The
        // application has a shutdown hook to close the connection pool, but
        // that is only run once the JVM terminates. Since all tests are run
        // within the same JVM, we manually close down the connection pool at
        // the end of each test.
        try {
            Constants.CONCOURSE_CONNECTIONS.close();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testOwnersAreLinks() {
        // In this test, we register variables that we want to debug in case the
        // test fails. All registered variables are output the console whenever
        // a test fails.
        Customer a = Variables.register("a", new ConcourseCustomer("Jeff",
                "Nelson"));
        Customer b = Variables.register("b", new ConcourseCustomer("Ashleah",
                "Nelson"));
        Customer c = Variables.register("c", new ConcourseCustomer("John",
                "Doe"));
        Account acct = new ConcourseAccount(250.15, a, b, c);
        Set<Link> links = client.select("owners", acct.getId());
        Variables.register("links", links);
        Assert.assertTrue(links.contains(Link.to(a.getId())));
        Assert.assertTrue(links.contains(Link.to(b.getId())));
        Assert.assertTrue(links.contains(Link.to(c.getId())));
        String ccl = MessageFormat.format("{0} {1} AND {0} {2} AND {0} {3}",
                "owners lnks2", Long.toString(a.getId()),
                Long.toString(b.getId()), Long.toString(c.getId()));
        Assert.assertEquals(Sets.newHashSet(acct.getId()), client.find(ccl));
    }

    @Test
    public void testDeposit() {
        Customer cust = new ConcourseCustomer("Jeff", "Nelson");
        Account acct = new ConcourseAccount(100.00, cust);
        Assert.assertEquals(100.00, client.get("balance", acct.getId()));
        acct.deposit(100);
        Assert.assertEquals(200.00, client.get("balance", acct.getId()));
    }

    @Test
    public void testWithdrawal() {
        Customer cust = new ConcourseCustomer("Jeff", "Nelson");
        Account acct = new ConcourseAccount(100.00, cust);
        Assert.assertTrue(acct.withdraw(40.38));
        Assert.assertEquals(59.62, client.get("balance", acct.getId()));
        Assert.assertTrue(client.describe(acct.getId()).contains("charges"));
    }

    @Test
    public void testCannotOverdraft() {
        Customer cust = new ConcourseCustomer("Jeff", "Nelson");
        Account acct = new ConcourseAccount(100.00, cust);
        Assert.assertFalse(acct.withdraw(200.00));
    }

    @Test
    public void testOverdraftProtection() {
        Customer cust1 = new ConcourseCustomer("Jeff", "Nelson");
        Customer cust2 = new ConcourseCustomer("Ashleah", "Nelson");
        Account checking = new ConcourseAccount(100, cust1, cust2);
        Account savings1 = new ConcourseAccount(100, cust1);
        Account savings2 = new ConcourseAccount(100, cust1);
        Assert.assertTrue(checking.withdraw(230.00));
        Assert.assertTrue(checking.getBalance() >= 0);
        Assert.assertTrue(savings1.getBalance() >= 0);
        Assert.assertTrue(savings2.getBalance() >= 0);
        Assert.assertEquals(
                70.00,
                checking.getBalance() + savings1.getBalance()
                        + savings2.getBalance(), 0);

    }

    @Test
    public void testOverdraftProtectionNotPossible() {
        Customer cust1 = new ConcourseCustomer("Jeff", "Nelson");
        Customer cust2 = new ConcourseCustomer("Ashleah", "Nelson");
        Account checking = new ConcourseAccount(100, cust1, cust2);
        Account savings1 = new ConcourseAccount(100, cust1);
        Account savings2 = new ConcourseAccount(100, cust1);
        Assert.assertFalse(checking.withdraw(330.00));
        Assert.assertEquals(100.00, checking.getBalance(), 0);
        Assert.assertEquals(100.00, savings1.getBalance(), 0);
        Assert.assertEquals(100.00, savings2.getBalance(), 0);
    }
}
