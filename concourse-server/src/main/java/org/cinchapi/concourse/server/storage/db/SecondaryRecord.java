/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
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
import org.cinchapi.concourse.util.MultimapViews;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

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
final class SecondaryRecord extends BrowsableRecord<Text, Value, PrimaryKey> {

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
        return explore(true, timestamp, operator, values).keySet();
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
        return explore(false, 0, operator, values).keySet();
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the Values
     * that cause the corresponding records to satisfy {@code operator} in
     * relation to the specified {@code values} at {@code timestamp}.
     * 
     * @param timestamp
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    public Map<PrimaryKey, Set<Value>> explore(long timestamp,
            Operator operator, Value... values) {
        return explore(true, timestamp, operator, values);
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the
     * Values that cause the corresponding records to satisfy {@code operator}
     * in relation to the specified {@code values}.
     * 
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    public Map<PrimaryKey, Set<Value>> explore(Operator operator,
            Value... values) {
        return explore(false, 0, operator, values);
    }

    @Override
    protected Map<Value, Set<PrimaryKey>> mapType() {
        return Maps.newTreeMap(Value.Sorter.INSTANCE);
    }

    /**
     * Explore this record and return a mapping from PrimaryKey to the Values
     * that cause the corresponding records to satisfy {@code operator} in
     * relation to the specified {@code values} (and at the specified
     * {@code timestamp} if {@code historical} is {@code true}).
     * 
     * @param historical - if {@code true} query the history, otherwise query
     *            the current state
     * @param timestamp - this value is ignored if {@code historical} is
     *            {@code false}, otherwise this value is the historical
     *            timestamp at which to query the field
     * @param operator
     * @param values
     * @return the relevant data that causes the matching records to satisfy the
     *         criteria
     */
    private Map<PrimaryKey, Set<Value>> explore(boolean historical,
            long timestamp, Operator operator, Value... values) { /* Authorized */
        read.lock();
        try {
            Map<PrimaryKey, Set<Value>> data = Maps.newHashMap();
            Value value = values[0];
            if(operator == Operator.EQUALS) {
                for (PrimaryKey record : historical ? get(value, timestamp)
                        : get(value)) {
                    MultimapViews.put(data, record, value);
                }
            }
            else if(operator == Operator.NOT_EQUALS) {
                for (Value stored : historical ? history.keySet() : present
                        .keySet()) {
                    if(!value.equals(stored)) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.GREATER_THAN) {
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).tailSet(
                                value, false)) {
                    if(!historical || stored.compareTo(value) > 0) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).tailSet(
                                value, true)) {
                    if(!historical || stored.compareTo(value) >= 0) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.LESS_THAN) {
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).headSet(
                                value, false)) {
                    if(!historical || stored.compareTo(value) < 0) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.LESS_THAN_OR_EQUALS) {
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).headSet(
                                value, true)) {
                    if(!historical || stored.compareTo(value) <= 0) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.BETWEEN) {
                Preconditions.checkArgument(values.length > 1);
                Value value2 = values[1];
                for (Value stored : historical ? history.keySet()
                        : ((NavigableSet<Value>) present.keySet()).subSet(
                                value, true, value2, false)) {
                    if(!historical
                            || (stored.compareTo(value) >= 0 && stored
                                    .compareTo(value2) < 0)) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.REGEX) {
                Pattern p = Pattern.compile(value.getObject().toString());
                for (Value stored : historical ? history.keySet() : present
                        .keySet()) {
                    Matcher m = p.matcher(stored.getObject().toString());
                    if(m.matches()) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else if(operator == Operator.NOT_REGEX) {
                Pattern p = Pattern.compile(value.getObject().toString());
                for (Value stored : historical ? history.keySet() : present
                        .keySet()) {
                    Matcher m = p.matcher(stored.getObject().toString());
                    if(!m.matches()) {
                        for (PrimaryKey record : historical ? get(stored,
                                timestamp) : get(stored)) {
                            MultimapViews.put(data, record, stored);
                        }
                    }
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
            return data;
        }
        finally {
            read.unlock();
        }
    }

}
