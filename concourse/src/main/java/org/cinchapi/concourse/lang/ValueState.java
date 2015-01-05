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

import org.cinchapi.concourse.Timestamp;

/**
 * The {@link State} that expects the current token to be the last or the next
 * token to be a value or conjunction specification.
 * 
 * @author jnelson
 */
public class ValueState extends BuildableState {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected ValueState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Add the specified {@code value} to the {@link Criteria} that is building.
     * 
     * @param value
     * @return the builder
     */
    public ValueState value(Object value) {
        criteria.add(ValueSymbol.create(value));
        return new ValueState(criteria);
    }
    
    /**
     * Add the specified {@code timestamp} to the {@link Criteria} that is building.
     * @param timestamp
     * @return the builder
     */
    public TimestampState at(Timestamp timestamp){
        criteria.add(TimestampSymbol.create(timestamp));
        return new TimestampState(criteria);
    }

}
