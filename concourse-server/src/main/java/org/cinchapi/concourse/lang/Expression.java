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

import java.text.MessageFormat;
import java.util.List;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.collect.Lists;

/**
 * An {@link Expression} is a {@link Symbol} that describes a query operation
 * on a key with respect to one or more values (e.g. key = value, key >=
 * value, etc).
 * <p>
 * This class is designed to make it easier to process the results of the
 * Shunting-Yard algorithm.
 * </p>
 * 
 * @author jnelson
 */
public class Expression extends AbstractSymbol implements PostfixNotationSymbol {

    /**
     * Create a new {@link Expression} that groups the specified {@code key},
     * {@code operator} and {@code values}.
     * 
     * @param key
     * @param operator
     * @param values
     * @return the symbol
     */
    public static Expression create(KeySymbol key, OperatorSymbol operator,
            ValueSymbol... values) {
        return new Expression(key, operator, values);
    }

    private final KeySymbol key;
    private final OperatorSymbol operator;
    private final List<ValueSymbol> values;
    private long timestamp = 0; // default timestamp value of 0 indicates
                                // this is a present state query

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param operator
     * @param values
     */
    public Expression(KeySymbol key, OperatorSymbol operator,
            ValueSymbol... values) {
        this.key = key;
        this.operator = operator;
        this.values = Lists.newArrayList(values);
    }

    /**
     * Return the key associated with this {@link Expression}.
     * 
     * @return the key
     */
    public KeySymbol getKey() {
        return key;
    }

    /**
     * Return the raw key associated with this {@link Expression}.
     * 
     * @return the key
     */
    public String getKeyRaw() {
        return getKey().getKey();
    }

    /**
     * Return the operator associated with this {@link Expression}.
     * 
     * @return the operator
     */
    public OperatorSymbol getOperator() {
        return operator;

    }

    /**
     * Return the raw operator associated with this {@link Expression}.
     * 
     * @return the operator
     */
    public Operator getOperatorRaw() {
        return getOperator().getOperator();
    }

    /**
     * Return the values associated with this {@link Expression}.
     * 
     * @return the values
     */
    public List<ValueSymbol> getValues() {
        return values;
    }

    /**
     * Add a {@code timestamp} to this {@link Expression}.
     * 
     * @param timestamp
     */
    public void setTimestamp(TimestampSymbol timestamp) {
        this.timestamp = timestamp.getTimestamp();
    }

    /**
     * Return the raw timestamp associated with this {@link Expression}.
     * 
     * @return the timestamp
     */
    public long getTimestampRaw() {
        return timestamp;
    }

    /**
     * Return the raw values associated with this {@link Expression}.
     * 
     * @return the values
     */
    public TObject[] getValuesRaw() {
        TObject[] values = new TObject[getValues().size()];
        for (int i = 0; i < values.length; ++i) {
            values[i] = getValues().get(i).getValue();
        }
        return values;
    }

    @Override
    public String toString() {
        String string = MessageFormat.format("{0} {1}", key, operator);
        for (ValueSymbol value : values) {
            string += " " + value;
        }
        if(timestamp > 0) {
            string += " at " + timestamp;
        }
        return string;
    }

}
