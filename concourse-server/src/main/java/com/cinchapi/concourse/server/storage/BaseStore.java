/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;

/**
 * The {@link Store} that provides basic functionality to all of its children.
 * 
 * @author Jeff Nelson
 */
public abstract class BaseStore implements Store {

    @Override
    public final Set<String> describe(long record) {
        return select(record).keySet();
    }

    @Override
    public final Set<String> describe(long record, long timestamp) {
        return select(record, timestamp).keySet();
    }

    @Override
    public final Map<Long, Set<TObject>> explore(long timestamp, String key,
            Operator operator, TObject... values) {
        for (int i = 0; i < values.length; ++i) {
            values[i] = Stores.normalizeValue(operator, values[i]);
        }
        operator = Stores.normalizeOperator(operator);
        return doExplore(timestamp, key, operator, values);
    }

    @Override
    public final Map<Long, Set<TObject>> explore(String key, Operator operator,
            TObject... values) {
        for (int i = 0; i < values.length; ++i) {
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
    
    @Override
    public Set<Long> getAllRecords(){
        throw new UnsupportedOperationException();
    }

}
