/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.importer.debug;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Diff;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TSets;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * A {@link Concourse} API that holds {@link #insert(String) inserted} data in
 * memory.
 * <p>
 * This particular API is designed to allow the import framework to perform
 * dry-run data imports.
 * </p>
 * <p>
 * <strong>NOTE:</strong> By default, this class intentionally contains
 * <em>static state.</em>. In order to support multi-threaded imports, this
 * class maintains a global view of data that has been inserted from multiple
 * instances.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ImportDryRunConcourse extends Concourse {

    /**
     * The imported data across ALL instances.
     */
    private static final List<Multimap<String, Object>> IMPORTED = Lists
            .newArrayList();

    /**
     * The list that holds each of the imported records, represented as a
     * multimap.
     */
    private final List<Multimap<String, Object>> imported;

    /**
     * Construct a new instance.
     */
    public ImportDryRunConcourse() {
        this(false);
    }

    /**
     * Construct a new instance.
     * 
     * @param isolated
     */
    public ImportDryRunConcourse(boolean isolated) {
        imported = isolated ? Lists.newArrayList() : IMPORTED;
    }

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
    public Map<Timestamp, Set<Object>> chronologize(String key, long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record,
            Timestamp start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Timestamp, Set<Object>> chronologize(String key, long record,
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
        synchronized (imported) {
            Set<String> keys = Sets.newHashSet();
            imported.forEach(record -> keys.addAll(record.keySet()));
            return keys;
        }
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

    /**
     * Dump the data that was inserted as a JSON blob.
     * 
     * @return a json dump
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();
        synchronized (imported) {
            sb.append("[");
            for (Multimap<String, Object> map : imported) {
                sb.append(Convert.mapToJson(map)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            return sb.toString();
        }

    }

    @Override
    public void exit() {}

    @Override
    public Set<Long> find(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(Object criteria) {
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
    public Set<Long> find(String key, Object value, Timestamp timestamp) {
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
            Object value2, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, Operator operator, Object value,
            Timestamp timestamp) {
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
            Object value2, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> find(String key, String operator, Object value,
            Timestamp timestamp) {
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
            Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
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
            Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            Object criteria, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(Object criteria,
            Timestamp timestamp) {
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
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Criteria criteria,
            Timestamp timestamp) {
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
    public <T> Map<Long, T> get(String key, Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, Object criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, T> get(String key, String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, T>> get(String ccl, Timestamp timestamp) {
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
        List<Multimap<String, Object>> data = Convert.anyJsonToJava(json);
        if(!data.isEmpty()) {
            synchronized (imported) {
                imported.addAll(data);
                long start = imported.size() + 1;
                long end = start + data.size() - 1;
                return TSets.sequence(start, end);
            }
        }
        else {
            return Collections.emptySet();
        }
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
    public Map<Long, Map<String, Set<Object>>> select(
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, Map<String, Set<Object>>> select(Collection<Long> records,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Collection<Long> records, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Criteria criteria, Timestamp timestamp) {
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
            Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            Object criteria, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<String> keys,
            String ccl, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Set<Object>> select(long record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Set<Object>> select(long record, Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Object criteria,
            Timestamp timestamp) {
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
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Criteria criteria,
            Timestamp timestamp) {
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
    public <T> Map<Long, Set<T>> select(String key, Object criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, Object criteria,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Set<T>> select(String key, String ccl,
            Timestamp timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(String ccl,
            Timestamp timestamp) {
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
        return new ImportDryRunConcourse();
    }

}
