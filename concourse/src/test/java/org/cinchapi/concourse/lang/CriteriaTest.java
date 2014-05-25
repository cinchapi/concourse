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
package org.cinchapi.concourse.lang;

import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Test;

/**
 * Unit tests for the {@link Criteria} building functionality.
 * 
 * @author jnelson
 */
public class CriteriaTest {

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltCriteria() {
        Criteria criteria = Criteria.builder().key("foo")
                .operator(Operator.EQUALS).value("bar").build();
        criteria.add(KeySymbol.create("baz"));
    }

    public static void main(String... args) {
        Criteria criteria = Criteria
                .builder()
                .group(Criteria
                        .builder()
                        .key("name")
                        .operator(Operator.EQUALS)
                        .value("jeff")
                        .and()
                        .group(Criteria.builder().key("age")
                                .operator(Operator.GREATER_THAN).value(2).or()
                                .key("age").operator(Operator.EQUALS).value(-1)
                                .build()).build()).build();
        System.out.println(criteria);
    }

}
