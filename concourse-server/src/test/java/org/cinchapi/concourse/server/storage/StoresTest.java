/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Unit tests for the {@link Stores} utilities.
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public class StoresTest {

    @DataPoints
    public static Operator[] operators = Operator.values();

    @Test
    @Theory
    public void testNormalizeOperator(Operator operator) {
        Operator expected = operator == Operator.LINKS_TO ? Operator.EQUALS
                : operator;
        Assert.assertEquals(expected, Stores.normalizeOperator(operator));
    }

    @Test
    @Theory
    public void testNormalizeValue(Operator operator) {
        long value = TestData.getLong();
        Object expected = operator == Operator.LINKS_TO ? Link.to(value)
                : value;
        Assert.assertEquals(Convert.javaToThrift(expected),
                Stores.normalizeValue(operator, Convert.javaToThrift(value)));
    }

    @Test
    public void testNormalizeLinksToNotLong() {
        TObject value = Convert.javaToThrift(TestData.getString());
        Assert.assertEquals(value,
                Stores.normalizeValue(Operator.LINKS_TO, value));
    }

}
