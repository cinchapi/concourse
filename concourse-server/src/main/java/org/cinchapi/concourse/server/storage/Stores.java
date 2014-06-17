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

/**
 * {@link Store} based utility functions.
 * 
 * @author jnelson
 */
public final class Stores {

    /**
     * Perform any necessary normalization on {@code operator} so that it can be
     * properly utilized in {@link Store} methods (i.e. convert a utility
     * operator to a functional one).
     * 
     * @param operator
     * @return the normalized Operator
     */
    public static Operator normalizeOperator(Operator operator) {
        return operator == Operator.LINKS_TO ? Operator.EQUALS : operator;
    }

    /**
     * Perform any necessary normalization on the {@code value} based on the
     * {@code operator}.
     * 
     * @param operator
     * @param values
     */
    public static TObject normalizeValue(Operator operator, TObject value) {
        try {
            return operator == Operator.LINKS_TO ? Convert.javaToThrift(Link
                    .to(((Number) Convert.thriftToJava(value)).longValue()))
                    : value;
        }
        catch (ClassCastException e) {
            return value;
        }
    }

}
