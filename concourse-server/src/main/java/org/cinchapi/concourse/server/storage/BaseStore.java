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

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

/**
 * The {@link Store} that provides basic functionality to all of its children.
 * 
 * @author jnelson
 */
public abstract class BaseStore implements Store {

    @Override
    public final Set<String> describe(long record) {
        return browse(record).keySet();
    }

    @Override
    public final Set<String> describe(long record, long timestamp) {
        return browse(record, timestamp).keySet();
    }

    @Override
    public final Map<Long, Set<TObject>> explore(long timestamp, String key,
            Operator operator, TObject... values) {
        for (int i = 0; i < values.length; i++) {
            values[i] = Stores.normalizeValue(operator, values[i]);
        }
        operator = Stores.normalizeOperator(operator);
        return doExplore(timestamp, key, operator, values);
    }

    @Override
    public final Map<Long, Set<TObject>> explore(String key, Operator operator,
            TObject... values) {
        for (int i = 0; i < values.length; i++) {
            values[i] = Stores.normalizeValue(operator, values[i]);
        }
        operator = Stores.normalizeOperator(operator);
        return doExplore(key, operator, values);
    }

    @Override
    public final Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        return explore(timestamp, key, operator, values).keySet();
    }

    @Override
    public final Set<Long> find(String key, Operator operator,
            TObject... values) {
        return explore(key, operator, values).keySet();
    }

    /**
     * Do the work to explore {@code key} {@code operator} {@code values} at
     * {@code timestamp} without worry about normalizing the operator or values.
     * 
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return a possibly empty map of data
     */
    protected abstract Map<Long, Set<TObject>> doExplore(long timestamp,
            String key, Operator operator, TObject... values);

    /**
     * Do the work to explore {@code key} {@code operator} {@code values}
     * without worrying about normalizing the operator or values.
     * 
     * @param key
     * @param operator
     * @param values
     * @return a possibly empty map of data
     */
    protected abstract Map<Long, Set<TObject>> doExplore(String key,
            Operator operator, TObject... values);

}
