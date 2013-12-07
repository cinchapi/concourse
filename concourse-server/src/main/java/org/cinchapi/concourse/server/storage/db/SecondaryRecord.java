/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.db;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A grouping of data for efficient indirect queries.
 * <p>
 * Each SecondaryRecord maps a value to a set of PrimaryKeys and provides an
 * interface for querying.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
final class SecondaryRecord extends Record<Text, Value, PrimaryKey> {

    /**
     * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
     * {@link Record#createSecondaryRecordPartial(Text, Value)} instead.
     * 
     * @param locator
     * @param key
     */
    @DoNotInvoke
    @PackagePrivate
    SecondaryRecord(Text locator, @Nullable Value key) {
        super(locator, key);
    }

    /**
     * Return the PrimaryKeys that satisfied {@code operator} in relation to the
     * specified {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param operator
     * @param values
     * @return the Set of PrimaryKeys that match the query
     */
    public Set<PrimaryKey> find(long timestamp, Operator operator,
            Value... values) {
        return find(true, timestamp, operator, values);
    }

    /**
     * Return the PrimaryKeys that <em>currently</em> satisfy {@code operator}
     * in relation to the specified {@code values}.
     * 
     * @param operator
     * @param values
     * @return they Set of PrimaryKeys that match the query
     */
    public Set<PrimaryKey> find(Operator operator, Value... values) {
        return find(false, 0, operator, values);
    }

    @Override
    protected Map<Value, Set<PrimaryKey>> mapType() {
        return Maps.newTreeMap(Value.Sorter.INSTANCE);
    }

    /**
     * Return the Set of PrimaryKeys that currently satisfy {@code operator} in
     * relation to the specified {@code values} or at the specified
     * {@code timestamp} if {@code historical} is {@code true}
     * 
     * @param historical - if {@code true} query the history for each field,
     *            otherwise query the current state
     * @param timestamp - this value is ignored if {@code historical} is
     *            {@code false}, otherwise this value is the historical
     *            timestamp at which to query the field
     * @param operator
     * @param values
     * @return the Set of PrimaryKeys that match the query
     */
    private Set<PrimaryKey> find(boolean historical, long timestamp,
            Operator operator, Value... values) { /* Authorized */
        read.lock();
        try {
            Set<PrimaryKey> keys = Sets.newTreeSet();
            Value value = values[0];
            if(operator == Operator.EQUALS) {
                keys.addAll(historical ? get(value, timestamp) : get(value));
            }
            else if(operator == Operator.NOT_EQUALS) {
                Iterator<Value> it = history.keySet().iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    if(!value.equals(v)) {
                        keys.addAll(historical ? get(v, timestamp) : get(v));
                    }
                }
            }
            else if(operator == Operator.GREATER_THAN) {
                TreeSet<Value> sortedValues = Sets
                        .newTreeSet(Value.Sorter.INSTANCE);
                sortedValues.addAll(history.keySet());
                Iterator<Value> it = sortedValues.tailSet(value, false)
                        .iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    keys.addAll(historical ? get(v, timestamp) : get(v));
                }
            }
            else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
                TreeSet<Value> sortedValues = Sets
                        .newTreeSet(Value.Sorter.INSTANCE);
                sortedValues.addAll(history.keySet());
                Iterator<Value> it = sortedValues.tailSet(value, true)
                        .iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    keys.addAll(historical ? get(v, timestamp) : get(v));
                }
            }
            else if(operator == Operator.LESS_THAN) {
                TreeSet<Value> sortedValues = Sets
                        .newTreeSet(Value.Sorter.INSTANCE);
                sortedValues.addAll(history.keySet());
                Iterator<Value> it = sortedValues.headSet(value, false)
                        .iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    keys.addAll(historical ? get(v, timestamp) : get(v));
                }
            }
            else if(operator == Operator.LESS_THAN_OR_EQUALS) {
                TreeSet<Value> sortedValues = Sets
                        .newTreeSet(Value.Sorter.INSTANCE);
                sortedValues.addAll(history.keySet());
                Iterator<Value> it = sortedValues.headSet(value, true)
                        .iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    keys.addAll(historical ? get(v, timestamp) : get(v));
                }
            }
            else if(operator == Operator.BETWEEN) {
                Preconditions.checkArgument(values.length > 1);
                Value value2 = values[1];
                TreeSet<Value> sortedValues = Sets
                        .newTreeSet(Value.Sorter.INSTANCE);
                sortedValues.addAll(history.keySet());
                Iterator<Value> it = sortedValues.subSet(value, true, value2,
                        false).iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    keys.addAll(historical ? get(v, timestamp) : get(v));
                }
            }
            else if(operator == Operator.REGEX) {
                Iterator<Value> it = history.keySet().iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    Pattern p = Pattern.compile(value.getObject().toString());
                    Matcher m = p.matcher(v.getObject().toString());
                    if(m.matches()) {
                        keys.addAll(historical ? get(v, timestamp) : get(v));
                    }
                }
            }
            else if(operator == Operator.NOT_REGEX) {
                Iterator<Value> it = history.keySet().iterator();
                while (it.hasNext()) {
                    Value v = it.next();
                    Pattern p = Pattern.compile(value.getObject().toString());
                    Matcher m = p.matcher(v.getObject().toString());
                    if(m.matches()) {
                        keys.addAll(historical ? get(v, timestamp) : get(v));
                    }
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
            return keys;
        }
        finally {
            read.unlock();
        }
    }

}
