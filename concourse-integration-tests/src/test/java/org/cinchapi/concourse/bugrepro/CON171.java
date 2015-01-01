/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
import org.cinchapi.concourse.TransactionException;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Repro of issue described in CON-171 where a phantom read is possible when
 * using transactions.
 * 
 * @author jnelson
 */
public class CON171 extends ConcourseIntegrationTest {

    @Test(expected=TransactionException.class)
    public void repro1() {
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client.find("foo", Operator.EQUALS, "bar");
        client2.set("foo", "bar", 1);
        Assert.assertTrue(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }
    
    @Test(expected=TransactionException.class)
    public void repro2(){
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client.find("foo", Operator.EQUALS, "bar");
        client2.add("foo", "bar", 1);
        Assert.assertTrue(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }
    
    @Test(expected=TransactionException.class)
    public void repro3(){
        Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        client.stage();
        client2.add("foo", "bar", 1);
        client.find("foo", Operator.EQUALS, "bar");
        client2.remove("foo", "bar", 1);
        Assert.assertFalse(client.find("foo", Operator.EQUALS, "bar").isEmpty());
    }

}
