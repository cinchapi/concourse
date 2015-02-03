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

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;

/**
 * The {@link State} that expects the next token to be an operator.
 * 
 * @author jnelson
 */
public class KeyState extends State {

    /**
     * Construct a new instance.
     * 
     * @param criteria
     */
    protected KeyState(Criteria criteria) {
        super(criteria);
    }

    /**
     * Add the specified {@code operator} to the {@link Criteria} that is
     * building.
     * 
     * @param operator
     * @return the builder
     */
    public OperatorState operator(Operator operator) {
        criteria.add(OperatorSymbol.create(operator));
        return new OperatorState(criteria);
    }
    
    public OperatorState operator(String operator) {
    	return operator(Convert.stringToOperator(operator));
    }

}
