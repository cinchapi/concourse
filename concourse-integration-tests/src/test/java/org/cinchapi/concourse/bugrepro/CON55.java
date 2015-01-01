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

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;
import org.junit.Assert;
import org.junit.Test;

/**
 * Repro of <a href="https://cinchapi.atlassian.net/browse/CON-55">CON-55</a>
 * where a transaction deadlock would occur when range reading a key before
 * adding data against that key.
 * 
 * @author jnelson
 */
public class CON55 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        client.stage();
        client.find("ipeds_id", Operator.EQUALS, Convert.stringToJava("1"));
        long record = client.create();
        client.add("ipeds_id", Convert.stringToJava("1"), record);
        client.add("avg_net_price_income_below_30000",
                Convert.stringToJava("15759"), record);
        client.add("avg_net_price_income_30001_to_48000",
                Convert.stringToJava("17292"), record);
        client.add("avg_net_price_income_48001_to_75000",
                Convert.stringToJava("19059"), record);
        client.add("avg_net_price_income_75001_110000",
                Convert.stringToJava("19734"), record);
        client.add("avg_net_price_income_above_110000",
                Convert.stringToJava("23351"), record);
        Assert.assertTrue(client.commit());
    }

}
