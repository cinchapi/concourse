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

import java.util.Map;

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.server.storage.Transaction;
import org.cinchapi.concourse.thrift.TransactionToken;
import org.cinchapi.concourse.util.Reflection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test to reproduce the memory leak issue described in CON-217.
 * 
 * @author jnelson
 */
public class CON217 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        client.stage();
        client.stage();
        Map<TransactionToken, Transaction> transactions = Reflection.get(
                "transactions", Reflection.get("server", this));
        Assert.assertEquals(1, transactions.size());
    }

}
