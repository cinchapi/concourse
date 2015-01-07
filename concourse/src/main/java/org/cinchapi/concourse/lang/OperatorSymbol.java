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

import java.text.MessageFormat;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;

/**
 * A {@link Symbol} that represents an {@link Operator} in a {@link Criteria}.
 * 
 * @author jnelson
 */
@Immutable
class OperatorSymbol extends AbstractSymbol implements PostfixNotationSymbol {

    /**
     * Return the {@link OperatorSymbol} for the specified {@code operator}.
     * 
     * @param operator
     * @return the symbol
     */
    public static OperatorSymbol create(Operator operator) {
        return new OperatorSymbol(operator);
    }

    /**
     * Return the {@link OperatorSymbol} that is parsed from {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static OperatorSymbol parse(String string) {
        Operator operator = Convert.stringToOperator(string);

        if(operator != null) {
            return new OperatorSymbol(operator);
        }
        else {
            throw new RuntimeException(MessageFormat.format(
                    "Cannot parse {0} into an OperatorSymbol", string));
        }
    }

    /**
     * The associated operator.
     */
    private final Operator operator;

    /**
     * Construct a new instance.
     * 
     * @param operator
     */
    private OperatorSymbol(Operator operator) {
        this.operator = operator;
    }

    /**
     * Return the operator represented by this {@link Symbol}.
     * 
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        return Convert.operatorToString(operator);
    }

}
