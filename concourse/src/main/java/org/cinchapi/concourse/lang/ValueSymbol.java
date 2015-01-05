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
package org.cinchapi.concourse.lang;

import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;

/**
 * A {@link Symbol} that represents a value in a {@link Criteria}.
 * 
 * @author jnelson
 */
class ValueSymbol extends AbstractSymbol {

    /**
     * Return the {@link ValueSymbol} for the specified {@code value}.
     * 
     * @param value
     * @return the symbol
     */
    public static ValueSymbol create(Object value) {
        return new ValueSymbol(value);
    }

    /**
     * Return the {@link ValueSymbol} that is parsed from {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static ValueSymbol parse(String string) {
        return new ValueSymbol(Convert.stringToJava(string));
    }

    /**
     * The associated value.
     */
    private final TObject value;

    /**
     * Construct a new instance.
     * 
     * @param value
     */
    private ValueSymbol(Object value) {
        this.value = Convert.javaToThrift(value);
    }

    /**
     * Return the value associated with this {@link Symbol}.
     * 
     * @return the value
     */
    public TObject getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

}
