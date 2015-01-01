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

import java.util.Set;

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests that reproduces the issue found in CON-167.
 * 
 * @author jnelson
 */
public class CON167 extends ConcourseIntegrationTest {

    @Test
    public void repro() {
        String value = "`bar`";
        client.set("foo", value, 1);
        client.set("foo", Tag.create(value), 2);
        Set<Long> expected = Sets.newHashSet(1L, 2L);
        Assert.assertEquals(expected,
                client.find("foo", Operator.EQUALS, value));
        Assert.assertEquals(
                expected,
                client.find(Criteria.where().key("foo")
                        .operator(Operator.EQUALS).value(value)));
    }

}
