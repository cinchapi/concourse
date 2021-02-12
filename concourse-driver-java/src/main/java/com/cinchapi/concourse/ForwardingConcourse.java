/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;

/**
 * An extensible {@link Concourse} wrapper that simply forwards method calls to
 * another {@link Concourse} instance.
 * <p>
 * This class is meant to be extended by a subclass that needs to override a
 * <strong>subset</strong> of methods to provide additional functionality before
 * or after the Concourse method execution. For example, an extension of this
 * class can be used to cache the return value of some Concourse methods and
 * check that cache before performing a database call.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("deprecation")
public abstract class ForwardingConcourse extends Concourse {

    /**
     * The instance to which method invocations are routed.
     */
    private final Concourse concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public ForwardingConcourse(Concourse concourse) {
        this.concourse = concourse;
    }

    @Override
    public void abort() {
        concourse.abort();
    }

    @Override
    public <T> long add(String key, T value) {
        return concourse.add(key, value);
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        return concourse.add(key, value, records);
    }

    @Override
    public <T> boolean add(String key, T value, long record) {
        return concourse.add(key, value, record);
    }

    @Override
    public Map<Timestamp, String> audit(long record) {
        return concourse.audit(record);
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start) {
        return concourse.audit(record, start);
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end) {
        return concourse.audit(record, start, end);
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record) {
        return concourse.audit(key, record);
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start) {
        return concourse.audit(key, record, start);
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end) {
        return concourse.audit(key, record, start, end);
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys) {
        return concourse.browse(keys);
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys,
            Timestamp timestamp) {
        return concourse.browse(keys, timestamp);
    }

    @Override
    public Map<Object, Set<Long>> browse(String key) {
        return concourse.browse(key);
    }

    @Override
    public Map<Object, Set<Long>> browse(String key, Timestamp timestamp) {
        return concourse.browse(key, timestamp);
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record) {
        return concourse.chronologize(key, record);
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
            Timestamp start) {
        return concourse.chronologize(key, record, start);
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
            Timestamp start, Timestamp end) {
        return concourse.chronologize(key, record, start, end);
    }

    @Override
    public void clear(Collection<Long> records) {
        concourse.clear(records);
    }

    @Override
    public void clear(Collection<String> keys, Collection<Long> records) {
        concourse.clear(keys, records);
    }

    @Override
    public void clear(Collection<String> keys, long record) {
        concourse.clear(keys, record);
    }

    @Override
    public void clear(long record) {
        concourse.clear(record);
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        concourse.clear(key, records);
    }

    @Override
    public void clear(String key, long record) {
        concourse.clear(key, record);
    }

    @Override
    public boolean commit() {
        return concourse.commit();
    }

    @Override
    public boolean consolidate(long first, long second, long... remaining) {
        return concourse.consolidate(first, second, remaining);
    }

    @Override
    public Set<String> describe() {
        return concourse.describe();
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records) {
        return concourse.describe(records);
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp) {
        return concourse.describe(records, timestamp);
    }

    @Override
    public Set<String> describe(long record) {
        return concourse.describe(record);
    }

    @Override
    public Set<String> describe(long record, Timestamp timestamp) {
        return concourse.describe(record, timestamp);
    }

    @Override
    public Set<String> describe(Timestamp timestamp) {
        return concourse.describe(timestamp);
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start) {
        return concourse.diff(record, start);
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record, Timestamp start,
            Timestamp end) {
        return concourse.diff(record, start, end);
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start) {
        return concourse.diff(key, record, start);
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record, Timestamp start,
            Timestamp end) {
        return concourse.diff(key, record, start, end);
    }

    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start) {
        return concourse.diff(key, start);
    }

    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start,
            Timestamp end) {
        return concourse.diff(key, start, end);
    }

    @Override
    public void exit() {
        concourse.exit();
    }

    @Override
    public Set<Long> find(Criteria criteria) {
        return concourse.find(criteria);
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order) {
        return concourse.find(criteria, order);
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order, Page page) {
        return concourse.find(criteria, order, page);
    }

    @Override
    public Set<Long> find(Criteria criteria, Page page) {
        return concourse.find(criteria, page);
    }

    @Override
    public Set<Long> find(String ccl) {
        return concourse.find(ccl);
    }

    @Override
    public Set<Long> find(String key, Object value) {
        return concourse.find(key, value);
    }

    @Override
    public Set<Long> find(String key, Object value, Order order) {
        return concourse.find(key, value, order);
    }

    @Override
    public Set<Long> find(String key, Object value, Order order, Page page) {
        return concourse.find(key, value, order, page);
    }

    @Override
    public Set<Long> find(String key, Object value, Page page) {
        return concourse.find(key, value, page);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp) {
        return concourse.find(key, value, timestamp);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order) {
        return concourse.find(key, value, timestamp, order);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order, Page page) {
        return concourse.find(key, value, timestamp, order, page);
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Page page) {
        return concourse.find(key, value, timestamp, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value) {
        return concourse.find(key, operator, value);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2) {
        return concourse.find(key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order) {
        return concourse.find(key, operator, value, value2, order);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order, Page page) {
        return concourse.find(key, operator, value, value2, order, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Page page) {
        return concourse.find(key, operator, value, value2, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp) {
        return concourse.find(key, operator, value, value2, timestamp);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        return concourse.find(key, operator, value, value2, timestamp, order);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        return concourse.find(key, operator, value, value2, timestamp, order,
                page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        return concourse.find(key, operator, value, value2, timestamp, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order) {
        return concourse.find(key, operator, value, order);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order, Page page) {
        return concourse.find(key, operator, value, order, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Page page) {
        return concourse.find(key, operator, value, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp) {
        return concourse.find(key, operator, value, timestamp);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order) {
        return concourse.find(key, operator, value, timestamp, order);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        return concourse.find(key, operator, value, timestamp, order, page);
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Page page) {
        return concourse.find(key, operator, value, timestamp, page);
    }

    @Override
    public Set<Long> find(String ccl, Order order) {
        return concourse.find(ccl, order);
    }

    @Override
    public Set<Long> find(String ccl, Order order, Page page) {
        return concourse.find(ccl, order, page);
    }

    @Override
    public Set<Long> find(String ccl, Page page) {
        return concourse.find(ccl, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value) {
        return concourse.find(key, operator, value);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2) {
        return concourse.find(key, operator, value, value2);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order) {
        return concourse.find(key, operator, value, value2, order);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order, Page page) {
        return concourse.find(key, operator, value, value2, order, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Page page) {
        return concourse.find(key, operator, value, value2, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp) {
        return concourse.find(key, operator, value, value2, timestamp);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        return concourse.find(key, operator, value, value2, timestamp, order);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        return concourse.find(key, operator, value, value2, timestamp, order,
                page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        return concourse.find(key, operator, value, value2, timestamp, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order) {
        return concourse.find(key, operator, value, order);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order, Page page) {
        return concourse.find(key, operator, value, order, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Page page) {
        return concourse.find(key, operator, value, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp) {
        return concourse.find(key, operator, value, timestamp);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order) {
        return concourse.find(key, operator, value, timestamp, order);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        return concourse.find(key, operator, value, timestamp, order, page);
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Page page) {
        return concourse.find(key, operator, value, timestamp, page);
    }

    @Override
    public <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException {
        return concourse.findOrAdd(key, value);
    }

    @Override
    public long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException {
        return concourse.findOrInsert(criteria, json);
    }

    @Override
    public long findOrInsert(String ccl, String json)
            throws DuplicateEntryException {
        return concourse.findOrInsert(ccl, json);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records) {
        return concourse.get(keys, records);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order) {
        return concourse.get(keys, records, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        return concourse.get(keys, records, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Page page) {
        return concourse.get(keys, records, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return concourse.get(keys, records, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        return concourse.get(keys, records, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        return concourse.get(keys, records, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        return concourse.get(keys, records, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria) {
        return concourse.get(keys, criteria);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order) {
        return concourse.get(keys, criteria, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        return concourse.get(keys, criteria, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Page page) {
        return concourse.get(keys, criteria, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return concourse.get(keys, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        return concourse.get(keys, criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        return concourse.get(keys, criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        return concourse.get(keys, criteria, timestamp, page);
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record) {
        return concourse.get(keys, record);
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record,
            Timestamp timestamp) {
        return concourse.get(keys, record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl) {
        return concourse.get(keys, ccl);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order) {
        return concourse.get(keys, ccl, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order, Page page) {
        return concourse.get(keys, ccl, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Page page) {
        return concourse.get(keys, ccl, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return concourse.get(keys, ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        return concourse.get(keys, ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        return concourse.get(keys, ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        return concourse.get(keys, ccl, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
        return concourse.get(criteria);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order) {
        return concourse.get(criteria, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order,
            Page page) {
        return concourse.get(criteria, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Page page) {
        return concourse.get(criteria, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp) {
        return concourse.get(criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order) {
        return concourse.get(criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return concourse.get(criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Page page) {
        return concourse.get(criteria, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl) {
        return concourse.get(ccl);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records) {
        return concourse.get(key, records);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order) {
        return concourse.get(key, records, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order, Page page) {
        return concourse.get(key, records, order, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Page page) {
        return concourse.get(key, records, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.get(key, records, timestamp);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        return concourse.get(key, records, timestamp, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return concourse.get(key, records, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        return concourse.get(key, records, timestamp, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria) {
        return concourse.get(key, criteria);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order) {
        return concourse.get(key, criteria, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order,
            Page page) {
        return concourse.get(key, criteria, order, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Page page) {
        return concourse.get(key, criteria, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp) {
        return concourse.get(key, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        return concourse.get(key, criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return concourse.get(key, criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        return concourse.get(key, criteria, timestamp, page);
    }

    @Override
    public <T> T get(String key, long record) {
        return concourse.get(key, record);
    }

    @Override
    public <T> T get(String key, long record, Timestamp timestamp) {
        return concourse.get(key, record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order) {
        return concourse.get(ccl, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order,
            Page page) {
        return concourse.get(ccl, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Page page) {
        return concourse.get(ccl, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl) {
        return concourse.get(key, ccl);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order) {
        return concourse.get(key, ccl, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order,
            Page page) {
        return concourse.get(key, ccl, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Page page) {
        return concourse.get(key, ccl, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
        return concourse.get(key, ccl, timestamp);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order) {
        return concourse.get(key, ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order, Page page) {
        return concourse.get(key, ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Page page) {
        return concourse.get(key, ccl, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
        return concourse.get(ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order) {
        return concourse.get(ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order, Page page) {
        return concourse.get(ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Page page) {
        return concourse.get(ccl, timestamp, page);
    }

    @Override
    public String getServerEnvironment() {
        return concourse.getServerEnvironment();
    }

    @Override
    public String getServerVersion() {
        return concourse.getServerVersion();
    }

    @Override
    public Set<Long> insert(String json) {
        return concourse.insert(json);
    }

    @Override
    public Map<Long, Boolean> insert(String json, Collection<Long> records) {
        return concourse.insert(json, records);
    }

    @Override
    public boolean insert(String json, long record) {
        return concourse.insert(json, record);
    }

    @Override
    public Set<Long> inventory() {
        return concourse.inventory();
    }

    @Override
    public <T> T invokePlugin(String id, String method, Object... args) {
        return concourse.invokePlugin(id, method, args);
    }

    @Override
    public String jsonify(Collection<Long> records) {
        return concourse.jsonify(records);
    }

    @Override
    public String jsonify(Collection<Long> records, boolean identifier) {
        return concourse.jsonify(records, identifier);
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp) {
        return concourse.jsonify(records, timestamp);
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp,
            boolean identifier) {
        return concourse.jsonify(records, timestamp, identifier);
    }

    @Override
    public String jsonify(long record) {
        return concourse.jsonify(record);
    }

    @Override
    public String jsonify(long record, boolean identifier) {
        return concourse.jsonify(record, identifier);
    }

    @Override
    public String jsonify(long record, Timestamp timestamp) {
        return concourse.jsonify(record, timestamp);
    }

    @Override
    public String jsonify(long record, Timestamp timestamp,
            boolean identifier) {
        return concourse.jsonify(record, timestamp, identifier);
    }

    @Override
    public Map<Long, Boolean> link(String key, Collection<Long> destinations,
            long source) {
        return concourse.link(key, destinations, source);
    }

    @Override
    public boolean link(String key, long destination, long source) {
        return concourse.link(key, destination, source);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Collection<Long> records) {
        return concourse.navigate(keys, records);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return concourse.navigate(keys, records, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria) {
        return concourse.navigate(keys, criteria);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return concourse.navigate(keys, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            long record) {
        return concourse.navigate(keys, record);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            long record, Timestamp timestamp) {
        return concourse.navigate(keys, record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            String ccl) {
        return concourse.navigate(keys, ccl);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return concourse.navigate(keys, ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key,
            Collection<Long> records) {
        return concourse.navigate(key, records);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.navigate(key, records, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria) {
        return concourse.navigate(key, criteria);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria,
            Timestamp timestamp) {
        return concourse.navigate(key, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, long record) {
        return concourse.navigate(key, record);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, long record,
            Timestamp timestamp) {
        return concourse.navigate(key, record, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, String ccl) {
        return concourse.navigate(key, ccl);
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, String ccl,
            Timestamp timestamp) {
        return concourse.navigate(key, ccl, timestamp);
    }

    @Override
    public Map<Long, Boolean> ping(Collection<Long> records) {
        return concourse.ping(records);
    }

    @Override
    public boolean ping(long record) {
        return concourse.ping(record);
    }

    @Override
    public <T> void reconcile(String key, long record, Collection<T> values) {
        concourse.reconcile(key, record, values);
    }

    @Override
    public <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records) {
        return concourse.remove(key, value, records);
    }

    @Override
    public <T> boolean remove(String key, T value, long record) {
        return concourse.remove(key, value, record);
    }

    @Override
    public void revert(Collection<String> keys, Collection<Long> records,
            Timestamp timestamp) {
        concourse.revert(keys, records, timestamp);
    }

    @Override
    public void revert(Collection<String> keys, long record,
            Timestamp timestamp) {
        concourse.revert(keys, record, timestamp);
    }

    @Override
    public void revert(String key, Collection<Long> records,
            Timestamp timestamp) {
        concourse.revert(key, records, timestamp);
    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        concourse.revert(key, record, timestamp);
    }

    @Override
    public Set<Long> search(String key, String query) {
        return concourse.search(key, query);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records) {
        return concourse.select(records);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Order order) {
        return concourse.select(records, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Order order, Page page) {
        return concourse.select(records, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Page page) {
        return concourse.select(records, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp) {
        return concourse.select(records, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Order order) {
        return concourse.select(records, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(records, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Page page) {
        return concourse.select(records, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records) {
        return concourse.select(keys, records);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order) {
        return concourse.select(keys, records, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        return concourse.select(keys, records, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Page page) {
        return concourse.select(keys, records, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        return concourse.select(keys, records, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        return concourse.select(keys, records, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        return concourse.select(keys, records, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        return concourse.select(keys, records, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria) {
        return concourse.select(keys, criteria);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order) {
        return concourse.select(keys, criteria, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        return concourse.select(keys, criteria, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Page page) {
        return concourse.select(keys, criteria, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        return concourse.select(keys, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        return concourse.select(keys, criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        return concourse.select(keys, criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        return concourse.select(keys, criteria, timestamp, page);
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys,
            long record) {
        return concourse.select(keys, record);
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys, long record,
            Timestamp timestamp) {
        return concourse.select(keys, record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl) {
        return concourse.select(keys, ccl);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order) {
        return concourse.select(keys, ccl, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order, Page page) {
        return concourse.select(keys, ccl, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Page page) {
        return concourse.select(keys, ccl, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        return concourse.select(keys, ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        return concourse.select(keys, ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        return concourse.select(keys, ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        return concourse.select(keys, ccl, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
        return concourse.select(criteria);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order) {
        return concourse.select(criteria, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order, Page page) {
        return concourse.select(criteria, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Page page) {
        return concourse.select(criteria, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp) {
        return concourse.select(criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order) {
        return concourse.select(criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Page page) {
        return concourse.select(criteria, timestamp, page);
    }

    @Override
    public <T> Map<String, Set<T>> select(long record) {
        return concourse.select(record);
    }

    @Override
    public <T> Map<String, Set<T>> select(long record, Timestamp timestamp) {
        return concourse.select(record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
        return concourse.select(ccl);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records) {
        return concourse.select(key, records);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order) {
        return concourse.select(key, records, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order, Page page) {
        return concourse.select(key, records, order, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Page page) {
        return concourse.select(key, records, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.select(key, records, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        return concourse.select(key, records, timestamp, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(key, records, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        return concourse.select(key, records, timestamp, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
        return concourse.select(key, criteria);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order) {
        return concourse.select(key, criteria, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order, Page page) {
        return concourse.select(key, criteria, order, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Page page) {
        return concourse.select(key, criteria, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp) {
        return concourse.select(key, criteria, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        return concourse.select(key, criteria, timestamp, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(key, criteria, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        return concourse.select(key, criteria, timestamp, page);
    }

    @Override
    public <T> Set<T> select(String key, long record) {
        return concourse.select(key, record);
    }

    @Override
    public <T> Set<T> select(String key, long record, Timestamp timestamp) {
        return concourse.select(key, record, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order) {
        return concourse.select(ccl, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order,
            Page page) {
        return concourse.select(ccl, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Page page) {
        return concourse.select(ccl, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl) {
        return concourse.select(key, ccl);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order) {
        return concourse.select(key, ccl, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order,
            Page page) {
        return concourse.select(key, ccl, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Page page) {
        return concourse.select(key, ccl, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp) {
        return concourse.select(key, ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order) {
        return concourse.select(key, ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(key, ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Page page) {
        return concourse.select(key, ccl, timestamp, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp) {
        return concourse.select(ccl, timestamp);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order) {
        return concourse.select(ccl, timestamp, order);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order, Page page) {
        return concourse.select(ccl, timestamp, order, page);
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Page page) {
        return concourse.select(ccl, timestamp, page);
    }

    @Override
    public void set(String key, Object value, Collection<Long> records) {
        concourse.set(key, value, records);
    }

    @Override
    public <T> void set(String key, T value, long record) {
        concourse.set(key, value, record);
    }

    @Override
    public void stage() throws TransactionException {
        concourse.stage();
    }

    @Override
    public Timestamp time() {
        return concourse.time();
    }

    @Override
    public Timestamp time(String phrase) {
        return concourse.time(phrase);
    }

    @Override
    public Map<Long, Map<String, Set<Long>>> trace(Collection<Long> records) {
        return concourse.trace(records);
    }

    @Override
    public Map<Long, Map<String, Set<Long>>> trace(Collection<Long> records,
            Timestamp timestamp) {
        return concourse.trace(records, timestamp);
    }

    @Override
    public Map<String, Set<Long>> trace(long record) {
        return concourse.trace(record);
    }

    @Override
    public Map<String, Set<Long>> trace(long record, Timestamp timestamp) {
        return concourse.trace(record, timestamp);
    }

    @Override
    public boolean unlink(String key, long destination, long source) {
        return concourse.unlink(key, destination, source);
    }

    @Override
    public boolean verify(String key, Object value, long record) {
        return concourse.verify(key, value, record);
    }

    @Override
    public boolean verify(String key, Object value, long record,
            Timestamp timestamp) {
        return concourse.verify(key, value, record, timestamp);
    }

    @Override
    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        return concourse.verifyAndSwap(key, expected, record, replacement);
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        concourse.verifyOrSet(key, value, record);
    }

    /**
     * Construct an instance of this {@link ForwardingConcourse} using the
     * provided {@code concourse} connection as the proxied handle.
     * 
     * @param concourse
     * @return an instace of this class
     */
    protected abstract ForwardingConcourse $this(Concourse concourse);

    @Override
    protected final Concourse copyConnection() {
        return $this(concourse.copyConnection());
    }

}
