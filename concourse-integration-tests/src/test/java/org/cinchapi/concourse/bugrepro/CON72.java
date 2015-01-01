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

import java.io.IOException;
import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Repro of <a href="https://cinchapi.atlassian.net/browse/CON-72">CON-72</a>
 * where a transaction deadlock would occur when writing a key as value before
 * range reading that key with the added value.
 * 
 * @author jnelson
 */
public class CON72 extends ConcourseIntegrationTest {

    @Test
    public void repro() throws IOException {
        client.stage();
        long record = client.create();
        client.set("__table__", "com.blavity.server.model.App", record);
        client.find("__table__", Operator.EQUALS,
                "com.blavity.server.model.App");
        Assert.assertTrue(client.commit());
    }

}
