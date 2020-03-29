/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
 * A {@link Concourse} extension that, by default, doesn't support any
 * functionality and is intended to be extended where there is a need to
 * implement a subset of functionality.
 *
 * @author Jeff Nelson
 */
public class NoOpConcourse extends Concourse {

    @Override
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> long add(String key, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> boolean add(String key, T value, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(long record, Timestamp start,
            Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, String> audit(String key, long record,
            Timestamp start, Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Map<Object, Set<Long>>> browse(Collection<String> keys,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Object, Set<Long>> browse(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Object, Set<Long>> browse(String key, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
            Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Timestamp, Set<T>> chronologize(String key, long record,
            Timestamp start, Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(Collection<String> keys, Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(Collection<String> keys, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean commit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> describe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Set<String>> describe(Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> describe(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> describe(long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> describe(Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record,
            Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Map<Diff, Set<T>>> diff(long record, Timestamp start,
            Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record,
            Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Diff, Set<T>> diff(String key, long record, Timestamp start,
            Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<T, Map<Diff, Set<Long>>> diff(String key, Timestamp start,
            Timestamp end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Object value, Timestamp timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Object value2, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> long findOrAdd(String key, T value)
            throws DuplicateEntryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long findOrInsert(Criteria criteria, String json)
            throws DuplicateEntryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long findOrInsert(String ccl, String json)
            throws DuplicateEntryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, T> get(Collection<String> keys, long record,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String key, long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServerEnvironment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServerVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> insert(String json) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Boolean> insert(String json, Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean insert(String json, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> inventory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokePlugin(String id, String method, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(Collection<Long> records, boolean identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(Collection<Long> records, Timestamp timestamp,
            boolean identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(long record, boolean identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String jsonify(long record, Timestamp timestamp,
            boolean identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Boolean> link(String key, Collection<Long> destinations,
            long source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean link(String key, long destination, long source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> navigate(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, long record,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> navigate(String key, String ccl,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Boolean> ping(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ping(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void reconcile(String key, long record, Collection<T> values) {
        throw new UnsupportedOperationException();

    }

    @Override
    public <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> boolean remove(String key, T value, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revert(Collection<String> keys, Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void revert(Collection<String> keys, long record,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void revert(String key, Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();

    }

    @Override
    public Set<Long> search(String key, String query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys,
            long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Set<T>> select(Collection<String> keys, long record,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Set<T>> select(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<String, Set<T>> select(long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Collection<Long> records,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> select(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> select(String key, long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(String key, Object value, Collection<Long> records) {
        throw new UnsupportedOperationException();

    }

    @Override
    public <T> void set(String key, T value, long record) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void stage() throws TransactionException {
        throw new UnsupportedOperationException();

    }

    @Override
    public Timestamp time() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp time(String phrase) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<Long>>> trace(Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<Long>>> trace(Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Set<Long>> trace(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Set<Long>> trace(long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unlink(String key, long destination, long source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean verify(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean verify(String key, Object value, long record,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Concourse copyConnection() {
        throw new UnsupportedOperationException();
    }

}
