/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.lang;

import java.util.List;

import com.cinchapi.concourse.lang.AbstractSymbol;
import com.cinchapi.concourse.lang.KeySymbol;
import com.cinchapi.concourse.lang.OperatorSymbol;
import com.cinchapi.concourse.lang.PostfixNotationSymbol;
import com.cinchapi.concourse.lang.Symbol;
import com.cinchapi.concourse.lang.TimestampSymbol;
import com.cinchapi.concourse.lang.ValueSymbol;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Strings;
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
 * @author Jeff Nelson
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
        String string = Strings.joinWithSpace(key, operator);
        for (ValueSymbol value : values) {
            string += " " + value;
        }
        if(timestamp > 0) {
            string += " at " + timestamp;
        }
        return string;
    }

}
