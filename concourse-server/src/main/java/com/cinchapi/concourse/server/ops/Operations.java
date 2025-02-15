/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.ops;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.cinchapi.ccl.syntax.AbstractSyntaxTree;
import com.cinchapi.common.base.StringSplitter;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.data.sort.Sortable;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.paginate.NoPage;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.NoOrder;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.ConcourseServer.DeferredWrite;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.calculate.Calculations;
import com.cinchapi.concourse.server.calculate.KeyCalculation;
import com.cinchapi.concourse.server.calculate.KeyRecordCalculation;
import com.cinchapi.concourse.server.query.Finder;
import com.cinchapi.concourse.server.query.paginate.Paging;
import com.cinchapi.concourse.server.query.sort.Sorting;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.ParseException;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Convert.ResolvableLink;
import com.cinchapi.concourse.util.DataServices;
import com.cinchapi.concourse.util.MultimapViews;
import com.cinchapi.concourse.util.Navigation;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TMaps;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

/**
 * A collection of auxiliary operations that are used in {@link ConcourseServer
 * ConcourseServer's} method implementations.
 * 
 * @author Jeff Nelson
 */
public final class Operations {

    /**
     * Add {@code key} as {@code value} in {@code record} using the atomic
     * {@code operation} if the record is empty. Otherwise, throw an
     * {@link AtomicStateException}.
     * <p>
     * If another operation adds data to the record after the initial check,
     * then an {@link AtomicStateException} will be thrown when an attempt is
     * made to commit {@code operation}.
     * </p>
     *
     * @param key
     * @param value
     * @param record
     * @param atomic
     * @throws AtomicStateException
     */
    public static void addIfEmptyAtomic(String key, TObject value, long record,
            AtomicOperation atomic) throws AtomicStateException {
        if(!atomic.contains(record)) {
            atomic.add(key, value, record);
        }
        else {
            throw AtomicStateException.RETRY;
        }
    }

    /**
     * Use the provided {@code store} to atomically add each of the values
     * stored across {@code key} at {@code timestamp} to the running
     * {@code sum}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the new running sum
     */
    public static Number avgKeyAtomic(String key, long timestamp, Store store) {
        checkAtomicity(store, timestamp);
        Map<TObject, Set<Long>> data = Stores.browse(store, key, timestamp);
        Number average = null;
        int count = 0;
        for (Entry<TObject, Set<Long>> entry : data.entrySet()) {
            TObject tobject = entry.getKey();
            Set<Long> records = entry.getValue();
            Object value = Convert.thriftToJava(tobject);
            Calculations.checkCalculatable(value);
            Number number = (Number) value;
            for (int i = 0; i < records.size(); ++i) {
                count++;
                average = average == null ? Numbers.divide(number, count)
                        : Numbers.incrementalAverage(average, number, count);
            }
        }
        return average;
    }

    /**
     * Use the provided {@code store} to atomically add each of the values in
     * {@code key}/{@code record} at {@code timestamp} to the running
     * {@code sum}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the new running sum
     */
    public static Number avgKeyRecordAtomic(String key, long record,
            long timestamp, Store store) {
        checkAtomicity(store, timestamp);
        Set<TObject> values = Stores.select(store, key, record, timestamp);
        Number sum = 0;
        for (TObject value : values) {
            Object object = Convert.thriftToJava(value);
            Calculations.checkCalculatable(object);
            Number number = (Number) object;
            sum = Numbers.add(sum, number);
        }
        return values.isEmpty() ? null : Numbers.divide(sum, values.size());
    }

    /**
     * Use the provided {@code store} to atomically add each of the values
     * stored for the {@code key} in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the new running sum
     */
    public static Number avgKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, Store store) {
        checkAtomicity(store, timestamp);
        int count = 0;
        Number avg = 0;
        for (long record : records) {
            Set<TObject> values = Stores.select(store, key, record, timestamp);
            for (TObject value : values) {
                Object object = Convert.thriftToJava(value);
                Calculations.checkCalculatable(object);
                Number number = (Number) object;
                count++;
                avg = Numbers.incrementalAverage(avg, number, count);
            }
        }
        return avg;
    }

    /**
     * Perform "browse" functionality on a navigation key.
     * 
     * @param key
     * @param timestamp
     * @param store
     * @return a mapping from each possible destination value for a given
     *         navigation {@code key} to the records where the navigation could
     *         begin to retrieve the value by selecting the navigation
     *         {@code key}
     */
    public static Map<TObject, Set<Long>> browseNavigationKeyOptionalAtomic(
            String key, long timestamp, Store store) {
        String[] toks = key.split("\\.");
        if(toks.length == 1) {
            return timestamp == Time.NONE ? store.browse(key)
                    : store.browse(key, timestamp);
        }
        else {
            String start = toks[0];
            StringBuilder $key = new StringBuilder();
            for (int i = 1; i < toks.length - 1; ++i) {
                $key.append(toks[i]).append('.');
            }
            $key.append(toks[toks.length - 1]);
            Map<TObject, Set<Long>> root = timestamp == Time.NONE
                    ? store.browse(start)
                    : store.browse(start, timestamp);
            Map<TObject, Set<Long>> index = Maps.newLinkedHashMap();
            root.entrySet().stream()
                    .filter(e -> e.getKey().getType() == Type.LINK)
                    .forEach(entry -> {
                        Link link = (Link) Convert.thriftToJava(entry.getKey());
                        Set<Long> nodes = entry.getValue();
                        for (long node : nodes) {
                            Set<TObject> values = traverseKeyRecordOptionalAtomic(
                                    $key.toString(), link.longValue(),
                                    timestamp, store);
                            for (TObject value : values) {
                                index.computeIfAbsent(value,
                                        ignore -> Sets.newLinkedHashSet())
                                        .add(node);
                            }
                        }
                    });
            return index;

        }
    }

    /**
     * Remove all the values mapped from the {@code key} in {@code record} using
     * the specified {@code atomic} operation.
     *
     * @param key
     * @param record
     * @param atomic
     */
    public static void clearKeyRecordAtomic(String key, long record,
            AtomicOperation atomic) {
        Set<TObject> values = atomic.select(key, record);
        for (TObject value : values) {
            atomic.remove(key, value, record);
        }
    }

    /**
     * Do the work to remove all the data from {@code record} using the
     * specified {@code atomic} operation.
     *
     * @param record
     * @param atomic
     */
    public static void clearRecordAtomic(long record, AtomicOperation atomic) {
        Map<String, Set<TObject>> values = atomic.select(record);
        for (Map.Entry<String, Set<TObject>> entry : values.entrySet()) {
            String key = entry.getKey();
            Set<TObject> valueSet = entry.getValue();
            for (TObject value : valueSet) {
                atomic.remove(key, value, record);
            }
        }
    }

    /**
     * Use the {@code store} to atomically compute the count across the
     * {@code key} at {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the count
     */
    public static long countKeyAtomic(String key, long timestamp, Store store) {
        return calculateKeyAtomic(key, timestamp, 0, store,
                Calculations.countKey()).longValue();
    }

    /**
     * Use the {@code store} to atomically compute the count of all the values
     * stored for {@code key} in {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the count
     */
    public static long countKeyRecordAtomic(String key, long record,
            long timestamp, Store store) {
        return calculateKeyRecordAtomic(key, record, timestamp, 0, store,
                Calculations.countKeyRecord()).longValue();
    }

    /**
     * Use the {@code store} to atomically compute the count of all the values
     * stored for {@code key} in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param records the record ids
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the count
     */
    public static long countKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, Store store) {
        long count = 0;
        for (long record : records) {
            count = calculateKeyRecordAtomic(key, record, timestamp, count,
                    store, Calculations.countKeyRecord()).longValue();
        }
        return count;
    }

    /**
     * Find data matching the criteria described by the {@code ast} or insert
     * each of the {@code objects} into a new record. Either way, place the
     * records that match the criteria or that contain the inserted data into
     * {@code records}.
     *
     * @param records - the collection that holds the records that either match
     *            the criteria or hold the inserted objects.
     * @param objects - a list of Multimaps, each of which containing data to
     *            insert into a distinct record. Get this using the
     *            {@link Convert#anyJsonToJava(String)} method.
     * @param ast - the parsed criteria attained from
     *            {@link com.cinchapi.ccl.Parser#parse(String)}.
     * @param atomic - the atomic operation through which all operations are
     *            conducted
     */
    public static void findOrInsertAtomic(Set<Long> records,
            List<Multimap<String, Object>> objects, AbstractSyntaxTree ast,
            AtomicOperation atomic) {
        records.addAll(ast.accept(Finder.instance(), atomic));
        if(records.isEmpty()) {
            List<DeferredWrite> deferred = Lists.newArrayList();
            for (Multimap<String, Object> object : objects) {
                long record = Time.now();
                atomic.touch(record);
                if(insertAtomic(object, record, atomic, deferred)) {
                    records.add(record);
                }
                else {
                    throw AtomicStateException.RETRY;
                }
            }
            insertDeferredAtomic(deferred, atomic);
        }
    }

    /**
     * From {@code store}, get the most recently stored value for each key in
     * each of the records that are resolved by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param ast
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of each of that record's keys
     *         associated with the most recently stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getAstAtomic(
            AtomicOperation store, AbstractSyntaxTree ast, Order order,
            Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getRecordsOptionalAtomic(store, records, Time.NONE, order, page,
                supplier);
    }

    /**
     * From {@code store}, get the most recently stored value at
     * {@code timestamp} for each key in each of the records that are resolved
     * by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of each of that record's keys
     *         associated with the most recently stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getAstOptionalAtomic(
            Store store, AbstractSyntaxTree ast, long timestamp, Order order,
            Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getRecordsOptionalAtomic(store, records, timestamp, order, page,
                supplier);
    }

    /**
     * From {@code store}, get the most recently stored value for {@code key} in
     * each of the records that are resolved by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier
     * @return the result set - a {@link Map} from each of the matching records
     *         to the most recently stored value for {@code key}
     */
    public static <M extends Map<Long, TObject> & Sortable<TObject>> Map<Long, TObject> getKeyAstAtomic(
            AtomicOperation store, String key, AbstractSyntaxTree ast,
            Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getKeyRecordsAtomic(store, key, records, order, page, supplier);
    }

    /**
     * From {@code store}, get the most recently stored value at
     * {@code timestamp} for {@code key} in each of the records that are
     * resolved by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier
     * @return the result set - a {@link Map} from each of the matching records
     *         to the most recently stored value for {@code key}
     */
    public static <M extends Map<Long, TObject> & Sortable<TObject>> Map<Long, TObject> getKeyAstOptionalAtomic(
            Store store, String key, AbstractSyntaxTree ast, long timestamp,
            Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getKeyRecordsOptionalAtomic(store, key, records, timestamp,
                order, page, supplier);
    }

    /**
     * From {@code store}, get the most recently stored value for {@code key} in
     * each of the {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to the most recently stored value for {@code key}
     */
    public static <M extends Map<Long, TObject> & Sortable<TObject>> Map<Long, TObject> getKeyRecordsAtomic(
            AtomicOperation store, String key, Iterable<Long> records,
            Order order, Page page, Supplier<M> supplier) {
        return getKeyRecordsOptionalAtomic(store, key, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, get the most recently stored value at
     * {@code timestamp} for {@code key} in each of the {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to the most recently stored value for {@code key}
     */
    public static <M extends Map<Long, TObject> & Sortable<TObject>> Map<Long, TObject> getKeyRecordsOptionalAtomic(
            Store store, String key, Iterable<Long> records, long timestamp,
            Order order, Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            try {
                Set<TObject> values = timestamp == Time.NONE
                        ? Stores.select(store, key, record)
                        : Stores.select(store, key, record, timestamp);
                TObject value = Iterables.getLast(values);
                data.put(record, value);
            }
            catch (NoSuchElementException e) {
                continue;
            }
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValue(order, store));
        }
        else {
            data.sort(Sorting.byValue(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * From {@code store}, and for each of the records resolved by the
     * {@code ast}, get the most recently stored value for each of the
     * {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param ast
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the most recently
     *         stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getKeysAstAtomic(
            AtomicOperation store, Collection<String> keys,
            AbstractSyntaxTree ast, Order order, Page page,
            Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getKeysRecordsOptionalAtomic(store, keys, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the records resolved by the
     * {@code ast}, get the most recently stored value for each of the
     * {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the most recently
     *         stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getKeysAstOptionalAtomic(
            Store store, Collection<String> keys, AbstractSyntaxTree ast,
            long timestamp, Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return getKeysRecordsOptionalAtomic(store, keys, records, timestamp,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, get the most
     * recently stored value for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the most recently
     *         stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getKeysRecordsAtomic(
            AtomicOperation store, Collection<String> keys,
            Iterable<Long> records, Order order, Page page,
            Supplier<M> supplier) {
        return getKeysRecordsOptionalAtomic(store, keys, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, get the most
     * recently stored value at {@code timestamp} for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the most recently
     *         stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getKeysRecordsOptionalAtomic(
            Store store, Collection<String> keys, Iterable<Long> records,
            long timestamp, Order order, Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            Map<String, TObject> row = (timestamp == Time.NONE
                    ? Stores.select(store, keys, record)
                    : Stores.select(store, keys, record, timestamp)).entrySet()
                            .stream()
                            .filter(entry -> !entry.getValue().isEmpty())
                            .collect(Collectors.toMap(Entry::getKey,
                                    entry -> Iterables
                                            .getLast(entry.getValue()),
                                    (a, b) -> b, LinkedHashMap::new));
            if(!row.isEmpty()) {
                data.put(record, row);
            }
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValue(order, store));
        }
        else {
            data.sort(Sorting.byValue(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * From {@code store}, get the most recently stored value for each key in
     * the provided {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of each of that record's keys
     *         associated with the most recently stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getRecordsAtomic(
            AtomicOperation store, Iterable<Long> records, Order order,
            Page page, Supplier<M> supplier) {
        return getRecordsOptionalAtomic(store, records, Time.NONE, order, page,
                supplier);
    }

    /**
     * From {@code store}, get the most recently stored value at
     * {@code timestamp} for each key in the provided {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of each of that record's keys
     *         associated with the most recently stored value for that key
     */
    public static <M extends Map<Long, Map<String, TObject>> & Sortable<TObject>> Map<Long, Map<String, TObject>> getRecordsOptionalAtomic(
            Store store, Iterable<Long> records, long timestamp, Order order,
            Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            Map<String, TObject> row = (timestamp == Time.NONE
                    ? store.select(record)
                    : store.select(record, timestamp)).entrySet().stream()
                            .filter(e -> !e.getValue().isEmpty())
                            .collect(Collectors.toMap(Entry::getKey,
                                    e -> Iterables.getLast(e.getValue())));
            if(!row.isEmpty()) {
                data.put(record, row);
            }
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValue(order, store));
        }
        else {
            data.sort(Sorting.byValue(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * Do the work to atomically insert all of the {@code data} into
     * {@code record} and return {@code true} if the operation is successful.
     *
     * @param data
     * @param record
     * @param atomic
     * @param deferred
     * @return {@code true} if all the data is atomically inserted
     */
    public static boolean insertAtomic(Multimap<String, Object> data,
            long record, AtomicOperation atomic, List<DeferredWrite> deferred) {
        for (String key : data.keySet()) {
            if(key.equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
                continue;
            }
            for (Object value : data.get(key)) {
                if(value instanceof ResolvableLink) {
                    deferred.add(new DeferredWrite(key, value, record));
                }
                else if(!atomic.add(key, Convert.javaToThrift(value), record)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Atomically insert a list of {@link DeferredWrite deferred writes}. This
     * method should only be called after all necessary calls to
     * {@link #insertAtomic(Multimap, long, AtomicOperation, List)} have been
     * made.
     *
     * @param parser
     * @param deferred
     * @param atomic
     * @return {@code true} if all the writes are successful
     */
    public static boolean insertDeferredAtomic(List<DeferredWrite> deferred,
            AtomicOperation atomic) {
        // NOTE: The validity of the key in each deferred write is assumed to
        // have already been checked
        for (DeferredWrite write : deferred) {
            if(write.getValue() instanceof ResolvableLink) {
                ResolvableLink rlink = (ResolvableLink) write.getValue();
                AbstractSyntaxTree ast = ConcourseCompiler.get()
                        .parse(rlink.getCcl());
                Set<Long> targets = ast.accept(Finder.instance(), atomic);
                for (long target : targets) {
                    if(target == write.getRecord()) {
                        // Here, if the target and source are the same, we skip
                        // instead of failing because we assume that the caller
                        // is using a complex resolvable link criteria that
                        // accidentally creates self links.
                        continue;
                    }
                    TObject link = Convert.javaToThrift(Link.to(target));
                    if(!atomic.add(write.getKey(), link, write.getRecord())) {
                        return false;
                    }
                }
            }
            else if(!atomic.add(write.getKey(),
                    Convert.javaToThrift(write.getValue()),
                    write.getRecord())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Do the work to jsonify (dump to json string) each of the {@code records},
     * possibly at {@code timestamp} (if it is greater than 0) using the
     * {@code store}.
     *
     * @param records
     * @param timestamp
     * @param includeId - will include the primary key for each record in the
     *            dump, if set to {@code true}
     * @param store
     * @return the json string dump
     */
    public static String jsonify(List<Long> records, long timestamp,
            boolean includeId, Store store) {
        JsonArray array = new JsonArray();
        for (long record : records) {
            Map<String, Set<TObject>> data = timestamp == 0
                    ? store.select(record)
                    : store.select(record, timestamp);
            JsonElement object = DataServices.gson().toJsonTree(data);
            if(includeId) {
                object.getAsJsonObject().addProperty(
                        GlobalState.JSON_RESERVED_IDENTIFIER_NAME, record);
            }
            array.add(object);
        }
        return array.size() == 1 ? array.get(0).toString() : array.toString();
    }

    /**
     * Use the {@code store} to atomically compute the max across all the values
     * stored for {@code key} across all records at {@code timestamp}.
     * 
     * @param key
     * @param timestamp
     * @param store
     * @return the max
     */
    public static Number maxKeyAtomic(String key, long timestamp, Store store) {
        checkAtomicity(store, timestamp);
        Map<TObject, Set<Long>> data = Stores.browse(store, key, timestamp);
        TObject max = Iterables.getLast(data.keySet(), null);
        if(max != null) {
            return (Number) Convert.thriftToJava(max);
        }
        else {
            return null;
        }
    }

    /**
     * Use the {@code store} to atomically compute the max across all the values
     * stored for {@code key} in {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the max
     */
    public static Number maxKeyRecordAtomic(String key, long record,
            long timestamp, Store store) {
        return calculateKeyRecordAtomic(key, record, timestamp, null, store,
                Calculations.maxKeyRecord());
    }

    /**
     * Use the {@code store} to atomically compute the max across all the values
     * stored for {@code key} in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param records the record ids
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the max
     */
    public static Number maxKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, Store store) {
        Number max = null;
        for (long record : records) {
            max = calculateKeyRecordAtomic(key, record, timestamp, max, store,
                    Calculations.maxKeyRecord());
        }
        return max;
    }

    /**
     * Use the {@code store} to atomically compute the min across all the values
     * stored for {@code key} across all records at {@code timestamp}.
     * 
     * @param key
     * @param timestamp
     * @param store
     * @return the max
     */
    public static Number minKeyAtomic(String key, long timestamp, Store store) {
        checkAtomicity(store, timestamp);
        Map<TObject, Set<Long>> data = Stores.browse(store, key, timestamp);
        TObject min = Iterables.getFirst(data.keySet(), null);
        if(min != null) {
            return (Number) Convert.thriftToJava(min);
        }
        else {
            return null;
        }
    }

    /**
     * Use the {@code store} to atomically compute the min across all the values
     * stored for {@code key} in {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the min
     */
    public static Number minKeyRecordAtomic(String key, long record,
            long timestamp, Store store) {
        return calculateKeyRecordAtomic(key, record, timestamp, null, store,
                Calculations.minKeyRecord());
    }

    /**
     * Use the {@code store} to atomically compute the min across all the values
     * stored for {@code key} in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param records the record ids
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the min
     */
    public static Number minKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, Store store) {
        Number min = null;
        for (long record : records) {
            min = calculateKeyRecordAtomic(key, record, timestamp, min, store,
                    Calculations.minKeyRecord());
        }
        return min;
    }

    /**
     * Do the work to atomically to navigate all the values for a key in a
     * record and if its of type {@value Type.LINK}, iterate until the key is
     * not {@value Type.LINK} at timestamp and return a {@code TObject} using
     * the provided atomic {@code operation}.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param atomic
     * @return a mapping from each record at the end of the navigation chain to
     *         the
     * @deprecated use
     *             {@link #traverseKeyRecordOptionalAtomic(String, long, long, Store)}
     *             instead
     */
    @Deprecated
    public static Map<Long, Set<TObject>> navigateKeyRecordAtomic(String key,
            long record, long timestamp, AtomicOperation atomic) {
        StringSplitter it = new StringSplitter(key, '.');
        Set<Long> records = Sets.newHashSet(record);
        Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
        while (it.hasNext()) {
            key = it.next();
            Set<Long> nextRecords = Sets.newLinkedHashSet();
            for (long rec : records) {
                Set<TObject> values = timestamp == Time.NONE
                        ? atomic.select(key, rec)
                        : atomic.select(key, rec, timestamp);
                if(!it.hasNext() && !values.isEmpty()) {
                    result.put(rec, values);
                }
                else {
                    values.forEach((value) -> {
                        if(value.type == Type.LINK) {
                            nextRecords.add(((Link) Convert.thriftToJava(value))
                                    .longValue());
                        }
                    });
                }
            }
            records = nextRecords;
        }
        return result;
    }

    /**
     * @deprecated use
     *             {@link #traverseKeyRecordsOptionalAtomic(String, Collection, long, Store)}
     *             instead
     */
    @Deprecated
    public static Map<Long, Set<TObject>> navigateKeyRecordsAtomic(String key,
            Set<Long> records, long timestamp, AtomicOperation atomic) {
        Map<Long, Set<TObject>> result = Maps.newLinkedHashMap();
        for (long record : records) {
            result.putAll(
                    navigateKeyRecordAtomic(key, record, timestamp, atomic));
        }
        return result;
    }

    /**
     * Do the work to atomically to navigate all the values for all keys in a
     * record and if its of type {@value Type.LINK}, iterate until the key is
     * not {@value Type.LINK} and return the result.
     * 
     * @param List<String> keys
     * @param record
     * @param atomic
     * @return Map<String, Set<TObject>> set of values.
     * @throws ParseException
     * @deprecated use
     *             {@link #traverseKeysRecordOptionalAtomic(Collection, long, long, Store)}
     *             instead
     */
    @Deprecated
    public static Map<Long, Map<String, Set<TObject>>> navigateKeysRecordAtomic(
            List<String> keys, long record, long timestamp,
            AtomicOperation atomic) {
        Map<Long, Map<String, Set<TObject>>> result = Maps.newLinkedHashMap();
        for (String k : keys) {
            Map<Long, Set<TObject>> data = navigateKeyRecordAtomic(k, record,
                    timestamp, atomic);
            String key = Navigation.getKeyDestination(k);
            data.forEach((rec, values) -> {
                Map<String, Set<TObject>> vals = result.get(rec);
                if(vals == null) {
                    vals = Maps.newLinkedHashMap();
                    result.put(rec, vals);
                }
                vals.put(key, values);
            });
        }
        return result;
    }

    /**
     * Do the work to atomically to navigate all the values for the specified
     * {@code keys} in the specified {@code records} and if its of type
     * {@value Type.LINK}, iterate until the key is not {@value Type.LINK} and
     * return the result.
     * 
     * @param List<String> keys
     * @param record
     * @param atomic
     * @return Map<String, Set<TObject>> set of values.
     * @throws ParseException
     * @deprecated use
     *             {@link #traverseKeysRecordsAtomic(Collection, Collection, long, Store)}
     *             instead
     */
    @Deprecated
    public static Map<Long, Map<String, Set<TObject>>> navigateKeysRecordsAtomic(
            List<String> keys, Set<Long> records, long timestamp,
            AtomicOperation atomic) {
        Map<Long, Map<String, Set<TObject>>> result = Maps.newLinkedHashMap();
        for (long record : records) {
            Map<Long, Map<String, Set<TObject>>> current = navigateKeysRecordAtomic(
                    keys, record, timestamp, atomic);
            current.forEach((rec, data) -> {
                Map<String, Set<TObject>> stored = result.get(rec);
                if(stored == null) {
                    result.put(rec, data);
                }
                else {
                    data.forEach((key, values) -> {
                        Set<TObject> vals = stored.get(key);
                        if(vals == null) {
                            stored.put(key, values);
                        }
                        else {
                            vals.addAll(values);
                        }
                    });
                }
            });
        }
        return result;

    }

    /**
     * Perform a ping of the {@code record} (e.g check to see if the record
     * currently has any data) from the perspective of the specified
     * {@code store}.
     *
     * @param record
     * @param store
     * @return {@code true} if the record currently has any data
     */
    public static boolean ping(long record, Store store) {
        return !store.describe(record).isEmpty();
    }

    /**
     * Revert {@code key} in {@code record} to its state {@code timestamp} using
     * the provided atomic {@code operation}.
     *
     * @param key
     * @param record
     * @param timestamp
     * @param atomic
     * @throws AtomicStateException
     */
    public static void revertAtomic(String key, long record, long timestamp,
            AtomicOperation atomic) throws AtomicStateException {
        Set<TObject> past = atomic.select(key, record, timestamp);
        Set<TObject> present = atomic.select(key, record);
        Set<TObject> xor = Sets.symmetricDifference(past, present);
        for (TObject value : xor) {
            if(present.contains(value)) {
                atomic.remove(key, value, record);
            }
            else {
                atomic.add(key, value, record);
            }
        }
    }

    /**
     * From {@code store}, select the values for each key in each of the records
     * that are resolved by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param ast
     * @param order
     * @param page
     * @param supplier
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of each of that record's keys
     *         associated with the values stored for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectAstAtomic(
            AtomicOperation store, AbstractSyntaxTree ast, Order order,
            Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectRecordsOptionalAtomic(store, records, Time.NONE, order,
                page, supplier);
    }

    /**
     * From {@code store}, select the values for each key in each of the records
     * that are resolved by the {@code ast}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of each of that record's keys
     *         associated with the values stored for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectAstOptionalAtomic(
            Store store, AbstractSyntaxTree ast, long timestamp, Order order,
            Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectRecordsOptionalAtomic(store, records, timestamp, order,
                page, supplier);
    }

    /**
     * From {@code store}, and for each of the records that are resolved by
     * {@code ast}, select the values stored for {@code key}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param ast
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a non-empty {@link Set} containing all of the
     *         values store for {@code key} in that record
     */
    public static <M extends Map<Long, Set<TObject>> & Sortable<Set<TObject>>> Map<Long, Set<TObject>> selectKeyAstAtomic(
            AtomicOperation store, String key, AbstractSyntaxTree ast,
            Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectKeyRecordsOptionalAtomic(store, key, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the records that are resolved by
     * {@code ast}, select the values stored for {@code key} at
     * {@code timestamp}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a non-empty {@link Set} containing all of the
     *         values store for {@code key} in that record
     */
    public static <M extends Map<Long, Set<TObject>> & Sortable<Set<TObject>>> Map<Long, Set<TObject>> selectKeyAstOptionalAtomic(
            Store store, String key, AbstractSyntaxTree ast, long timestamp,
            Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectKeyRecordsOptionalAtomic(store, key, records, timestamp,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, select the
     * values stored for {@code key}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a non-empty {@link Set} containing all of the
     *         values store for {@code key} in that record
     */
    public static <M extends Map<Long, Set<TObject>> & Sortable<Set<TObject>>> Map<Long, Set<TObject>> selectKeyRecordsAtomic(
            AtomicOperation store, String key, Iterable<Long> records,
            Order order, Page page, Supplier<M> supplier) {
        return selectKeyRecordsOptionalAtomic(store, key, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, select the
     * values stored at {@code timestamp} for {@code key}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param key
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a non-empty {@link Set} containing all of the
     *         values store for {@code key} in that record
     */
    public static <M extends Map<Long, Set<TObject>> & Sortable<Set<TObject>>> Map<Long, Set<TObject>> selectKeyRecordsOptionalAtomic(
            Store store, String key, Iterable<Long> records, long timestamp,
            Order order, Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            try {
                Set<TObject> values = timestamp == Time.NONE
                        ? Stores.select(store, key, record)
                        : Stores.select(store, key, record, timestamp);
                data.put(record, values);
            }
            catch (NoSuchElementException e) {
                continue;
            }
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValues(order, store));
        }
        else {
            data.sort(Sorting.byValues(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * From {@code store}, and for each of the records resolved by the
     * {@code ast}, get the values stored for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param ast
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the values stored
     *         value for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectKeysAstAtomic(
            AtomicOperation store, Collection<String> keys,
            AbstractSyntaxTree ast, Order order, Page page,
            Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectKeysRecordsOptionalAtomic(store, keys, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the records resolved by the
     * {@code ast}, get the values stored for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param ast
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         records to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the values stored
     *         value for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectKeysAstOptionalAtomic(
            Store store, Collection<String> keys, AbstractSyntaxTree ast,
            long timestamp, Order order, Page page, Supplier<M> supplier) {
        Set<Long> records = ast.accept(Finder.instance(), store);
        return selectKeysRecordsOptionalAtomic(store, keys, records, timestamp,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, get the values
     * stored for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the values stored
     *         value for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectKeysRecordsAtomic(
            AtomicOperation store, Collection<String> keys,
            Iterable<Long> records, Order order, Page page,
            Supplier<M> supplier) {
        return selectKeysRecordsOptionalAtomic(store, keys, records, Time.NONE,
                order, page, supplier);
    }

    /**
     * From {@code store}, and for each of the {@code records}, get the values
     * stored at {@code timestamp} for each of the {@code keys}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param keys
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of the {@code keys} in that
     *         record that are non-empty, associated with the values stored
     *         value for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectKeysRecordsOptionalAtomic(
            Store store, Collection<String> keys, Iterable<Long> records,
            long timestamp, Order order, Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            Map<String, Set<TObject>> row = timestamp == Time.NONE
                    ? Stores.select(store, keys, record)
                    : Stores.select(store, keys, record, timestamp);
            if(!row.isEmpty()) {
                TMaps.putResultDatasetOptimized(data, record, row);
            }
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValues(order, store));
        }
        else {
            data.sort(Sorting.byValues(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * From {@code store}, select the values for each key in the provided
     * {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param records
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of each of that record's keys
     *         associated with the values stored for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectRecordsAtomic(
            AtomicOperation store, Iterable<Long> records, Order order,
            Page page, Supplier<M> supplier) {
        return selectRecordsOptionalAtomic(store, records, Time.NONE, order,
                page, supplier);
    }

    /**
     * From {@code store}, select the values at {@code timestamp} for each key
     * in the provided {@code records}.
     * 
     * <p>
     * If possible, apply the {@code order} and {@code page} parameters to the
     * result set.
     * </p>
     * 
     * @param store
     * @param records
     * @param timestamp
     * @param order
     * @param page
     * @param supplier a {@link Supplier} of a {@link Sortable} in which the
     *            results can be gathered
     * @return the result set - a {@link Map} from each of the relevant
     *         {@code records} to a {@link Map} of each of that record's keys
     *         associated with the values stored for that key
     */
    public static <M extends Map<Long, Map<String, Set<TObject>>> & Sortable<Set<TObject>>> Map<Long, Map<String, Set<TObject>>> selectRecordsOptionalAtomic(
            Store store, Iterable<Long> records, long timestamp, Order order,
            Page page, Supplier<M> supplier) {
        M data = supplier.get();
        if(order instanceof NoOrder) {
            // If page == NoPage, this is a no-op; otherwise, apply the
            // pagination directly to the input records so that we don't fetch
            // more data than required.
            records = Paging.page(records, page);
            page = NoPage.INSTANCE;
        }
        for (long record : records) {
            Map<String, Set<TObject>> row = timestamp == Time.NONE
                    ? store.select(record)
                    : store.select(record, timestamp);
            TMaps.putResultDatasetOptimized(data, record, row);
        }
        if(timestamp == Time.NONE) {
            data.sort(Sorting.byValues(order, store));
        }
        else {
            data.sort(Sorting.byValues(order, store), timestamp);
        }

        // Assuming page != NoPage and pagination was not applied to the
        // input records (e.g., the data was actually sorted); otherwise this is
        // a no-op
        return Paging.page(data, page);
    }

    /**
     * Use the {@code store} to atomically compute the sum across the
     * {@code key} at {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the sum
     */
    public static Number sumKeyAtomic(String key, long timestamp, Store store) {
        return calculateKeyAtomic(key, timestamp, null, store,
                Calculations.sumKey());
    }

    /**
     * Use the {@code store} to atomically compute the sum across all the values
     * stored for {@code key} in {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the sum
     */
    public static Number sumKeyRecordAtomic(String key, long record,
            long timestamp, Store store) {
        return calculateKeyRecordAtomic(key, record, timestamp, null, store,
                Calculations.sumKeyRecord());
    }

    /**
     * Use the {@code store} to atomically compute the sum across all the values
     * stored for {@code key} in each of the {@code records} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param records the record ids
     * @param timestamp the selection timestamp
     * @param store the {@link Store} to use
     * @return the sum
     */
    public static Number sumKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, Store store) {
        Number sum = null;
        for (long record : records) {
            sum = calculateKeyRecordAtomic(key, record, timestamp, sum, store,
                    Calculations.sumKeyRecord());
        }
        return sum;
    }

    /**
     * Join the {@link AtomicOperation atomic} operation to trace all incoming
     * links to {@code record} at {@code timestamp}.
     *
     * @param record
     * @param timestamp
     * @param atomic
     * @return a mapping from each relevant key to the records where the key is
     *         stored as a link to {@code record}.
     */
    public static Map<String, Set<Long>> traceRecordAtomic(long record,
            long timestamp, AtomicOperation atomic) {
        Map<String, Set<Long>> incoming = Maps.newLinkedHashMap();
        for (long source : atomic.getAllRecords()) {
            if(record != source) {
                Map<String, Set<TObject>> data = timestamp == Time.NONE
                        ? atomic.select(source)
                        : atomic.select(source, timestamp);
                data.forEach((key, values) -> {
                    boolean isSource = values.stream()
                            .filter(value -> value.getType() == Type.LINK)
                            .map(Convert::thriftToJava).map(Link.class::cast)
                            .map(Link::longValue)
                            .filter(destination -> destination == record)
                            .limit(1).count() > 0;
                    if(isSource) {
                        MultimapViews.put(incoming, key, source);
                    }
                });
            }
        }
        return incoming;
    }

    /**
     * Join the {@link AtomicOperation atomic} operation to trace all incoming
     * links for each of the {@code records} at {@code timestamp}.
     * 
     * @param records
     * @param timestamp
     * @param atomic
     * @return a mapping from each of the {@code records} to a mapping from each
     *         relevant key to the records where the key is stored as a link to
     *         {@code record}.
     */
    public static Map<Long, Map<String, Set<Long>>> traceRecordsAtomic(
            Collection<Long> records, long timestamp, AtomicOperation atomic) {
        Map<Long, Map<String, Set<Long>>> incomings = Maps.newLinkedHashMap();
        for (long source : atomic.getAllRecords()) {
            Map<String, Set<TObject>> data = timestamp == Time.NONE
                    ? atomic.select(source)
                    : atomic.select(source, timestamp);
            data.forEach((key, values) -> {
                values.stream().filter(value -> value.getType() == Type.LINK)
                        .map(Convert::thriftToJava).map(Link.class::cast)
                        .map(Link::longValue).filter(records::contains)
                        .forEach(destination -> {
                            Map<String, Set<Long>> incoming = incomings
                                    .computeIfAbsent(destination,
                                            ignore -> Maps.newLinkedHashMap());
                            MultimapViews.put(incoming, key, source);
                        });
            });
        }
        return incomings;
    }

    /**
     * Atomically traverse a navigation {@code key} from {@code record} and
     * return the values that are at the end of the path.
     * 
     * @param key
     * @param record
     * @param timestamp
     * @param store
     * @return all the values that can be reached by traversing the document
     *         graph along {@code key} from {@code record}
     */
    public static Set<TObject> traverseKeyRecordOptionalAtomic(String key,
            long record, long timestamp, Store store) {
        String[] toks = key.split("\\.");
        Set<TObject> values = Sets.newLinkedHashSet();
        Set<Long> nodes = ImmutableSet.of(record);
        for (int i = 0; i < toks.length; ++i) {
            key = toks[i];
            Set<Long> descendents = Sets.newLinkedHashSet();
            for (long node : nodes) {
                Set<TObject> $values = timestamp == Time.NONE
                        ? Stores.select(store, key, node)
                        : Stores.select(store, key, node, timestamp);
                if(i == toks.length - 1) {
                    values.addAll($values);
                }
                else {
                    for (TObject $value : $values) {
                        if($value.getType() == Type.LINK) {
                            descendents
                                    .add(((Link) Convert.thriftToJava($value))
                                            .longValue());
                        }
                    }
                }
            }
            nodes = descendents;
        }
        return values;
    }

    /**
     * Atomically traverse a navigation {@code key} from each of the specified
     * {@code records} and map each to the values that are at the end of the
     * path.
     * 
     * @param key
     * @param records
     * @param timestamp
     * @param store
     * @return a mapping from each of the {@code records} to all of the values
     *         that can be reached by traversing the document graph along
     *         {@code key} from the record
     */
    public static Map<Long, Set<TObject>> traverseKeyRecordsOptionalAtomic(
            String key, Collection<Long> records, long timestamp, Store store) {
        Map<Long, Set<TObject>> data = Maps.newLinkedHashMap();
        for (long record : records) {
            Set<TObject> values = traverseKeyRecordOptionalAtomic(key, record,
                    timestamp, store);
            if(!values.isEmpty()) {
                data.put(record, values);
            }
        }
        return data;
    }

    /**
     * Atomically traverse each of the navigation {@code keys} from
     * {@code record} and map each key to the values that are at the end of the
     * path.
     * 
     * @param keys
     * @param record
     * @param timestamp
     * @param store
     * @return a mapping from each of the {@code keys} to all of the values that
     *         can be reached by traversing the document graph along the key
     *         from {@code record}
     */
    public static Map<String, Set<TObject>> traverseKeysRecordOptionalAtomic(
            Collection<String> keys, long record, long timestamp, Store store) {
        Map<String, Set<TObject>> data = Maps.newLinkedHashMap();
        for (String key : keys) {
            Set<TObject> values = traverseKeyRecordOptionalAtomic(key, record,
                    timestamp, store);
            if(!values.isEmpty()) {
                data.put(key, values);
            }
        }
        return data;
    }

    /**
     * Atomically traverse each of the navigation {@code keys} from
     * each of the {@code records} and map each record to a mapping of each key
     * to the values that are at the end of the path.
     * 
     * @param keys
     * @param records
     * @param timestamp
     * @param store
     * @return a mapping from each of the {@code records} to each of the
     *         {@code keys} to all of the values that can be reached by
     *         traversing the document graph
     */
    public static Map<Long, Map<String, Set<TObject>>> traverseKeysRecordsAtomic(
            Collection<String> keys, Collection<Long> records, long timestamp,
            Store store) {
        Map<Long, Map<String, Set<TObject>>> data = Maps.newLinkedHashMap();
        for (long record : records) {
            Map<String, Set<TObject>> entry = traverseKeysRecordOptionalAtomic(
                    keys, record, timestamp, store);
            if(!entry.isEmpty()) {
                data.put(record, entry);
            }
        }
        return data;
    }

    /**
     * Use the provided {@link AtomicOperation atomic} operation to perform the
     * specified {@code calculation} across the {@code key} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param result the running result
     * @param store the {@link AtomicOperation} to use
     * @param calculation the calculation logic
     * @return the result after applying the {@code calculation}
     */
    private static Number calculateKeyAtomic(String key, long timestamp,
            Number result, Store store, KeyCalculation calculation) {
        checkAtomicity(store, timestamp);
        Map<TObject, Set<Long>> data = Stores.browse(store, key, timestamp);
        for (Entry<TObject, Set<Long>> entry : data.entrySet()) {
            TObject tobject = entry.getKey();
            Set<Long> records = entry.getValue();
            Object value = Convert.thriftToJava(tobject);
            Calculations.checkCalculatable(value);
            result = calculation.calculate(result, (Number) value, records);
        }
        return result;
    }

    /**
     * Use the provided {@link AtomicOperation atomic} operation to perform the
     * specified {@code calculation} over the values stored for {@code key} in
     * {@code record} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param result the running result
     * @param store the {@link AtomicOperation} to use
     * @param calculation the calculation logic
     * @return the result after applying the {@code calculation}
     */
    private static Number calculateKeyRecordAtomic(String key, long record,
            long timestamp, Number result, Store store,
            KeyRecordCalculation calculation) {
        checkAtomicity(store, timestamp);
        Set<TObject> values = Stores.select(store, key, record, timestamp);
        for (TObject tobject : values) {
            Object value = Convert.thriftToJava(tobject);
            Calculations.checkCalculatable(value);
            result = calculation.calculate(result, (Number) value);
        }
        return result;
    }

    /**
     * Check to see if the {code store} provides the appropriate level of
     * atomicity to process an operation at {@code timestamp}. If it does not,
     * throw and {@link InsufficientAtomicityException}.
     * 
     * @param store
     * @param timestamp
     * @throws InsufficientAtomicityException
     */
    private static void checkAtomicity(Store store, long timestamp)
            throws InsufficientAtomicityException {
        if(timestamp == Time.NONE && !(store instanceof AtomicOperation)) {
            throw new InsufficientAtomicityException();
        }
    }

    private Operations() {/* no-op */}

}