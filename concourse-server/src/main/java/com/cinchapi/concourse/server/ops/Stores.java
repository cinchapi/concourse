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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.IndexFunction;
import com.cinchapi.ccl.type.function.KeyConditionFunction;
import com.cinchapi.ccl.type.function.KeyRecordsFunction;
import com.cinchapi.ccl.type.function.TemporalFunction;
import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.calculate.Calculations;
import com.cinchapi.concourse.server.ops.Strategy.Source;
import com.cinchapi.concourse.server.query.Finder;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.KeyValue;
import com.cinchapi.concourse.validate.Keys;
import com.cinchapi.concourse.validate.Keys.Key;
import com.cinchapi.concourse.validate.Keys.KeyType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * A collection of "smart" operations that delegate to functionality in a
 * {@link Store} in the most efficient way possible while maintaining data
 * consistency.
 *
 * @author Jeff Nelson
 */
public final class Stores {

    /**
     * Browse the values stored for {@code key} in the {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#browse(String) browse}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#browseNavigationKeyOptionalAtomic(String, long, Store)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param timestamp
     * 
     * @return the values stored for {@code key} across the {@code store}
     */
    public static Map<TObject, Set<Long>> browse(Store store, String key) {
        return browse(store, key, Time.NONE);
    }

    /**
     * Browse the values stored for {@code key} at {@code timestamp} in the
     * {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#browse(String) browse}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#browseNavigationKeyOptionalAtomic(String, long, Store)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param timestamp
     * 
     * @return the values stored for {@code key} across the {@code store}
     */
    public static Map<TObject, Set<Long>> browse(Store store, String key,
            long timestamp) {
        if(Keys.isNavigationKey(key)) {
            if(store instanceof AtomicOperation || timestamp != Time.NONE) {
                return Operations.browseNavigationKeyOptionalAtomic(key,
                        timestamp, store);
            }
            else if(store instanceof AtomicSupport) {
                AtomicReference<Map<TObject, Set<Long>>> data = new AtomicReference<>(
                        ImmutableMap.of());
                AtomicOperations.executeWithRetry((AtomicSupport) store,
                        (atomic) -> {
                            data.set(Operations
                                    .browseNavigationKeyOptionalAtomic(key,
                                            timestamp, atomic));
                        });
                return data.get();
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot browse the current values of a navigation key using a Store that does not support atomic operations");
            }
        }
        else if(key.equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
            return store.getAllRecords().stream().collect(Collectors.toMap(
                    Convert::javaToThrift, record -> ImmutableSet.of(record)));
        }
        else {
            return timestamp == Time.NONE ? store.browse(key)
                    : store.browse(key, timestamp);
        }
    }

    /**
     * Find the records that contain values that are stored for {@code key} and
     * satisfy {@code operator} in relation to the specified {@code values} at
     * {@code timestamp}.
     * <p>
     * If the {@code key} is primitive, the store lookup is usually a simple
     * {@link Store#find(String, Operator, TObject[]) find}. However, if the key
     * is a navigation key, this method will process it by
     * {@link #browse(Store, String) browsing} the destination values and
     * checking the operator validity of each if and only if the {@code store}
     * is an {@link AtomicOperation} or {@link AtomicSupport supports} starting
     * one.
     * </p>
     * 
     * @param store
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return the records that satisfy the query
     */
    public static Set<Long> find(Store store, long timestamp, String key,
            Operator operator, TObject... values) {
        Key $key = Keys.parse(key);
        KeyType keyType = $key.type();
        for (int i = 0; i < values.length; ++i) {
            TObject value = values[i];
            if(value.getType() == Type.FUNCTION) {
                Function function = (Function) Convert.thriftToJava(value);
                TemporalFunction func = (TemporalFunction) function;
                String method = Calculations.alias(function.operation());
                ArrayBuilder<Object> args = ArrayBuilder.builder();
                method += "Key";
                args.add(function.key());
                if(function instanceof KeyRecordsFunction
                        || function instanceof KeyConditionFunction) {
                    method += "Records";
                    Collection<Long> records = function instanceof KeyRecordsFunction
                            ? ((KeyRecordsFunction) function).source()
                            : Finder.instance().visit(
                                    ((KeyConditionFunction) function).source(),
                                    store);
                    args.add(records);
                }
                else if(!(function instanceof IndexFunction)) {
                    throw new IllegalStateException("Invalid function value");
                }
                method += "Atomic";
                args.add(func.timestamp());
                args.add(store);
                values[i] = Convert.javaToThrift(Reflection
                        .callStatic(Operations.class, method, args.build()));
            }
        }
        if(keyType == KeyType.NAVIGATION_KEY) {
            return findNavigationKey(NavigationKeyFinder.AUTO, store, timestamp,
                    $key, operator, values);
        }
        else if(keyType == KeyType.FUNCTION_KEY) {
            Set<Long> records = Sets.newLinkedHashSet();
            for (long record : store.getAllRecords()) {
                Set<TObject> aggregate = select(store, key, record, timestamp);
                for (TObject tobject : aggregate) {
                    if(tobject.is(operator, values)) {
                        records.add(record);
                        break;
                    }
                }
            }
            return records;
        }
        else if((operator == Operator.CONTAINS
                || operator == Operator.NOT_CONTAINS) && timestamp == Time.NONE
                && keyType == KeyType.WRITABLE_KEY) {
            Set<Long> matches = store.search(key, values[0]);
            if(operator == Operator.CONTAINS) {
                return matches;
            }
            else {
                return Sets.difference(store.getAllRecords(), matches);
            }
        }
        else {
            return timestamp == Time.NONE ? store.find(key, operator, values)
                    : store.find(timestamp, key, operator, values);
        }
    }

    /**
     * Find the records that contain values that are stored for {@code key} and
     * satisfy {@code operator} in relation to the specified {@code values}.
     * <p>
     * If the {@code key} is primitive, the store lookup is usually a simple
     * {@link Store#find(String, Operator, TObject[]) find}. However, if the key
     * is a navigation key, this method will process it by
     * {@link #browse(Store, String) browsing} the destination values and
     * checking the operator validity of each if and only if the {@code store}
     * is an {@link AtomicOperation} or {@link AtomicSupport supports} starting
     * one.
     * </p>
     * 
     * @param store
     * @param key
     * @param operator
     * @param values
     * @return the records that satisfy the query
     */
    public static Set<Long> find(Store store, String key, Operator operator,
            TObject... values) {
        return find(store, Time.NONE, key, operator, values);
    }

    /**
     * Select all of the {@code keys} from {@code record} within {@code store}.
     * <p>
     * This method contains optimizations to efficiently select multiple keys
     * from a record with as few lookups as possible; especially if there are
     * multiple {@link KeyType#NAVIGATION_KEY navigation keys}.
     * </p>
     * 
     * @param store
     * @param keys
     * @param record
     * @return a mapping from each of the {@code keys} to the data held for it
     *         in {@code record} within {@code store}
     */
    public static Map<String, Set<TObject>> select(Store store,
            Collection<String> keys, long record) {
        return select(store, keys, record, Time.NONE);
    }

    /**
     * Select all of the {@code keys} from {@code record} at {@code timestamp}
     * within {@code store}.
     * <p>
     * This method contains optimizations to efficiently select multiple keys
     * from a record with as few lookups as possible; especially if there are
     * multiple {@link KeyType#NAVIGATION_KEY navigation keys}.
     * </p>
     * 
     * @param store
     * @param keys
     * @param record
     * @param timestamp
     * @return a mapping from each of the {@code keys} to the data held for it
     *         in {@code record} at {@code timestamp} within {@code store}
     */
    public static Map<String, Set<TObject>> select(Store store,
            Collection<String> keys, long record, long timestamp) {
        // TODO: does this need an atomic operation?
        if(keys.isEmpty()) {
            return ImmutableMap.of();
        }
        else {
            Map<String, Set<TObject>> data = new HashMap<>(keys.size());
            Map<String, Set<TObject>> stored = null;
            Node root = null;
            int count = 1;
            store.advisoryLock().readLock().lock();
            try {
                for (String key : keys) {
                    Key metadata = Keys.parse(key);
                    KeyType type = metadata.type();
                    if(type == KeyType.NAVIGATION_KEY) {
                        // Generate a single Graph containing all of the stops
                        // in each of the navigation keys.
                        root = root == null ? Node.root(record) : root;
                        Node node = root;
                        String[] stops = metadata.data();
                        for (String stop : stops) {
                            node = node.next(stop);
                            ++count;
                        }
                        node.end();
                    }
                    else {
                        Set<TObject> values;
                        if(type == KeyType.WRITABLE_KEY && keys.size() == 1) {
                            // Since there is only one key and it is writable,
                            // tap into the Strategy framework to determine the
                            // most efficient lookup source.
                            return ImmutableMap.of(key, lookupWithStrategy(
                                    store, key, record, timestamp));
                        }
                        else if(type == KeyType.WRITABLE_KEY) {
                        // @formatter:off
                        stored = stored == null
                                ? (timestamp == Time.NONE 
                                      ? store.select(record)
                                      : store.select(record, timestamp)
                                  )
                                : stored;
                        // @formatter:on
                            values = stored.get(key);
                            if(values == null) {
                                values = ImmutableSet.of();
                            }
                        }
                        else if(type == KeyType.IDENTIFIER_KEY) {
                            values = ImmutableSet
                                    .of(Convert.javaToThrift(record));
                        }
                        else if(type == KeyType.FUNCTION_KEY) {
                            Function function = metadata.data();
                            String method = Calculations.alias(
                                    function.operation()) + "KeyRecordAtomic";
                            Number value = Reflection.callStatic(
                                    Operations.class, method, function.key(),
                                    record, timestamp, store);
                            values = value != null
                                    ? ImmutableSet
                                            .of(Convert.javaToThrift(value))
                                    : ImmutableSet.of();
                        }
                        else {
                            values = ImmutableSet.of();
                        }
                        data.put(key, values);
                    }
                }
                if(root != null) {
                    // Iterate through the graph, in a breadth-first manner, to
                    // perform bulk selection at each Junctions.
                    Queue<Node> queue = new ArrayDeque<>(count);
                    queue.add(root);
                    while (!queue.isEmpty()) {
                        Node node = queue.poll();
                        Collection<Node> successors = node.successors();
                        if(successors.isEmpty()) {
                            data.put(node.path, node.values());
                        }
                        else {
                            queue.addAll(successors);
                            Collection<Long> links = node.links();
                            for (long link : links) {
                                Map<String, Set<TObject>> intermediate = null;
                                if(successors.size() > 1) {
                                    // Bypassing the Strategy framework is
                                    // acceptable here because we know that
                                    // there are multiple keys that need to be
                                    // selected from each record, so it makes
                                    // sense to select the entire record from
                                    // the Engine, once
                                    intermediate = timestamp == Time.NONE
                                            ? store.select(link)
                                            : store.select(link, timestamp);
                                }
                                for (Node successor : successors) {
                                    String stop = successor.stop;
                                    if(intermediate == null) {
                                        // This means there is only 1 successor,
                                        // so the lookup should defer to the
                                        // Strategy framework
                                        intermediate = ImmutableMap.of(stop,
                                                lookupWithStrategy(store, stop,
                                                        link, timestamp));
                                    }
                                    Set<TObject> values = intermediate
                                            .get(stop);
                                    if(values != null) {
                                        successor.store(values);
                                    }
                                    else if(stop.equals(
                                            Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
                                        successor.store(
                                                Convert.javaToThrift(link));
                                    }
                                }
                            }
                        }
                    }
                }
                // @formatter:off
                return data.size() > 1
                        ? new OrderImposingMap<>(keys, data)
                        : data;
                // @formatter:on
            }
            finally {
                store.advisoryLock().readLock().unlock();
            }
        }
    }

    /**
     * Select the values stored for {@code key} in {@code record} from the
     * {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * 
     * @return the values stored for {@code key} in {@code record} according to
     *         the {@code store}
     */
    public static Set<TObject> select(Store store, String key, long record) {
        return select(store, ImmutableList.of(key), record).get(key);
    }

    /**
     * Select the values stored for {@code key} in {@code record} at
     * {@code timestamp} from the {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * @param timestamp
     * 
     * @return the values stored for {@code key} in {@code record} at
     *         {@code timestamp} according to the {@code store}
     */
    public static Set<TObject> select(Store store, String key, long record,
            long timestamp) {
        return select(store, ImmutableList.of(key), record, timestamp).get(key);
    }

    /**
     * Select the values stored for {@code key} in {@code record} from the
     * {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * 
     * @return the values stored for {@code key} in {@code record} according to
     *         the {@code store}
     */
    protected static Set<TObject> serialSelect(Store store, String key,
            long record) {
        return serialSelect(store, key, record, Time.NONE);
    }

    /**
     * Select the values stored for {@code key} in {@code record} at
     * {@code timestamp} from the {@code store}.
     * <p>
     * If the {@code key} is primitive, the store retrieval is usually a simple
     * {@link Store#select(long) select}. However, if the key is a navigation
     * key, this method will process it via
     * {@link Operations#traverseKeyRecordOptionalAtomic(String, long, long, AtomicOperation)}
     * if the {@code store} is an {@link AtomicOperation} or
     * {@link AtomicSupport supports} starting one.
     * </p>
     * 
     * @param store
     * @param key
     * @param record
     * @param timestamp
     * 
     * @return the values stored for {@code key} in {@code record} at
     *         {@code timestamp} according to the {@code store}
     */
    protected static Set<TObject> serialSelect(Store store, String key,
            long record, long timestamp) {
        Function evalFunc;
        if(Keys.isNavigationKey(key)) {
            if(store instanceof AtomicOperation || timestamp != Time.NONE) {
                return Operations.traverseKeyRecordOptionalAtomic(key, record,
                        timestamp, store);
            }
            else if(store instanceof AtomicSupport) {
                AtomicReference<Set<TObject>> value = new AtomicReference<>(
                        ImmutableSet.of());
                AtomicOperations.executeWithRetry((AtomicSupport) store,
                        (atomic) -> {
                            value.set(
                                    Operations.traverseKeyRecordOptionalAtomic(
                                            key, record, timestamp, atomic));
                        });
                return value.get();
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot fetch the current values of a navigation key using a Store that does not support atomic operations");
            }
        }
        else if((evalFunc = Keys.tryParseFunction(key)) != null) {
            String method = Calculations.alias(evalFunc.operation())
                    + "KeyRecordAtomic";
            Number value = Reflection.callStatic(Operations.class, method,
                    evalFunc.key(), record, timestamp, store);
            return value != null ? ImmutableSet.of(Convert.javaToThrift(value))
                    : ImmutableSet.of();
        }
        else if(key.equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
            return ImmutableSet.of(Convert.javaToThrift(record));
        }
        else {
            Source source;
            if(Command.isSet()) {
                Strategy strategy = new Strategy(Command.current(), store);
                source = strategy.source(key, record);
            }
            else {
                source = Source.FIELD;
            }
            Set<TObject> values;
            if(source == Source.RECORD) {
                // @formatter:off
                Map<String, Set<TObject>> data = timestamp == Time.NONE
                        ? store.select(record)
                        : store.select(record, timestamp);
                values = data.getOrDefault(key, ImmutableSet.of());
                // @formatter:on
            }
            else if(source == Source.FIELD) {
                // @formatter:off
                values = timestamp == Time.NONE ? store.select(key, record)
                        : store.select(key, record, timestamp);
                // @formatter:on
            }
            else { // source == Source.INDEX
                // @formatter:off
                values = timestamp == Time.NONE ? store.gather(key, record)
                        : store.gather(key, record, timestamp);
                // @formatter:on
            }
            return values;

        }
    }

    /**
     * Find the records that contain stored values under a navigation
     * {@code key} that satisfy {@code operator} in relation to the specified
     * {@code values} at {@code timestamp}.
     * <p>
     * The {@code finder} parameter instructs this method on the lookup
     * technique to use when processing the command. Use
     * {@link NavigationKeyFinder#AUTO} to select the most optimal path given
     * the nature of the stored data and the navigation path being traversed.
     * </p>
     * <p>
     * <strong>NOTE:</strong> The {@code store} must be an
     * {@link AtomicOperation} or one that {@link AtomicSupport supports}
     * starting one.
     * </p>
     * <p>
     * <em>Since this method is exposed for internal and testing purposes, there
     * is no verification that {@code key} is a {@link KeyType#NAVIGATION_KEY
     * NAVIGATION_KEY}.</em>
     * </p>
     * 
     * @param finder
     * @param store
     * @param timestamp
     * @param key
     * @param operator
     * @param values
     * @return the records that satisfy the condition
     */
    @VisibleForTesting
    static Set<Long> findNavigationKey(NavigationKeyFinder finder, Store store,
            long timestamp, Key key, Operator operator, TObject... values) {
        if(!(store instanceof AtomicOperation) && timestamp == Time.NONE) {
            if(store instanceof AtomicSupport) {
                return AtomicOperations.supplyWithRetry((AtomicSupport) store,
                        atomic -> findNavigationKey(finder, atomic, timestamp,
                                key, operator, values));
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot query the present data for a navigation key using a "
                                + "Store that does not support atomic operations");
            }
        }
        else if(finder == NavigationKeyFinder.ADHOC_INDEX) {
            Map<TObject, Set<Long>> index = timestamp == Time.NONE
                    ? browse(store, key.value())
                    : browse(store, key.value(), timestamp);
            Set<Long> records = index.entrySet().stream()
                    .filter(e -> e.getKey().is(operator, values))
                    .map(e -> e.getValue()).flatMap(Set::stream)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return records;
        }
        else {
            /*
             * ENHANCED NAVIGATION KEY FINDING:
             * Given navigation key (e.g., "user.posts.comments.text" =
             * "foo"), we can do an optimized find by:
             * 1. Parsing the key into individual stops (e.g., ["user",
             * "posts", "comments", "text"])
             * 2. Finding records that have outgoing links on the first stop
             * key ("user")
             * 3. Finding records matching the final condition (e.g., "text"
             * = "foo")
             * 4. Connecting these endpoints by traversing the navigation
             * path
             *
             * We have two possible traversal strategies:
             * - Forward traversal: Start with records having outgoing links
             * on the first stop and follow links forward
             * - Reverse traversal: Start with records matching the final
             * condition and work backwards
             *
             * The choice between strategies depends on the relative sizes
             * of the result sets:
             * - Forward is better when: (# of start records * # of stops)
             * <= # of end records
             * - Reverse is better when: (# of start records * # of stops) >
             * # of end records
             */
            String[] stops = key.data();

            // The "starts" are records that have outgoing links on the
            // first stop key. This creates a mapping from those link
            // destinations to the source records containing those links
            Stream<Entry<Long, Set<Long>>> starts = (timestamp == Time.NONE
                    ? browse(store, stops[0])
                    : browse(store, stops[0], timestamp)).entrySet().stream()
                            .filter(e -> e.getKey().getType() == Type.LINK)
                            .map(e -> {
                                Long destination = ((Link) Convert
                                        .thriftToJava(e.getKey())).longValue();
                                Set<Long> sources = e.getValue();
                                return new SimpleImmutableEntry<>(destination,
                                        sources);
                            });
            AtomicInteger numStarts = new AtomicInteger(0);
            starts = starts.peek($ -> numStarts.incrementAndGet())
                    .collect(Collectors.toList()).stream();

            // The "ends" are records that match the final condition in the
            // navigation path (e.g., where the last stop key satisfies the
            // operator in relation to the values
            Set<Long> $ends = find(store, timestamp, stops[stops.length - 1],
                    operator, values);
            int remainingStops = stops.length - 2;
            if(finder == NavigationKeyFinder.REVERSE_TRAVERSAL
                    || (finder == NavigationKeyFinder.AUTO
                            && (numStarts.get() * remainingStops) > $ends
                                    .size())) {
                /*
                 * REVERSE TRAVERSAL APPROACH:
                 * 1. Starting with each of the #ends and going backwards
                 * until reaching the starts, find records that link to each
                 * #end on the previous stop in the path.
                 * 2. For each end record, find records that link to it via
                 * the previous stop
                 * 
                 * In this approach, we only need to consult the index for
                 * each stop key along the path.
                 */
                for (int i = stops.length - 2; i > 0; --i) {
                    String stop = stops[i];
                    Set<Long> intermediates = new LinkedHashSet<>();
                    for (long end : $ends) {
                        Set<Long> intermediate = find(store, timestamp, stop,
                                Operator.LINKS_TO, Convert.javaToThrift(end));
                        intermediates.addAll(intermediate);
                    }
                    $ends = intermediates;
                }
            }
            else {
                /*
                 * FORWARD TRAVERSAL APPROACH:
                 * 1. For each of the #starts, follow their links to the
                 * second to last stop and map any of those values that are
                 * Links to the source record from which the traversal tarted
                 * (#intermediates)
                 * 2. Filter out any #intermediates that are not links to
                 * one of the #ends.
                 * 2. For each of these records, follow their links to the
                 * second to last stop.
                 */
                while (remainingStops > 0) {
                    String[] _stops = Arrays.copyOfRange(stops, 1,
                            1 + remainingStops);
                    String intermediatePath = String.join(".", _stops);
                    remainingStops -= _stops.length;
                    starts = starts.flatMap(e -> {
                        Long successor = e.getKey();
                        Set<Long> initialSources = e.getValue();
                        Set<TObject> intermediates = Operations
                                .traverseKeyRecordOptionalAtomic(
                                        intermediatePath, successor, timestamp,
                                        store);
                        // Replace each of the destinations in #starts with
                        // the #intermediate destinations, mapped to the
                        // initialSources. This way, we can simply check if
                        // those intermediate destinations are contained in
                        // the set of the #ends (e.g., matches to the ultimate
                        // find condition) that we captured above
                        return intermediates.stream()
                                .filter(_value -> _value.getType() == Type.LINK)
                                .map(_link -> ((Link) Convert
                                        .thriftToJava(_link)).longValue())
                                .map(_destination -> new SimpleImmutableEntry<>(
                                        _destination,
                                        new LinkedHashSet<>(initialSources)));
                    });
                }
            }
            Set<Long> ends = $ends instanceof HashSet ? $ends
                    : new LinkedHashSet<>($ends);
            starts = starts.filter(e -> ends.contains(e.getKey()));
            Set<Long> records = starts.map(e -> e.getValue())
                    .flatMap(Set::stream)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return records;
        }
    }

    /**
     * Use the {@link Strategy} framework to lookup {@code key} in
     * {@code record} at {@code timestamp} within {@code store}.
     * <p>
     * It is assumed that the {@code key} is a {@link KeyType#WRITABLE_KEY
     * writable key}.
     * </p>
     * 
     * 
     * @param store
     * @param key
     * @param record
     * @param timestamp
     * @return the set of {@link TObject values} stored in {@code store} for
     *         {@code key} in {@code record}.
     */
    private static Set<TObject> lookupWithStrategy(Store store, String key,
            long record, long timestamp) {
        Source source;
        if(Command.isSet()) {
            Strategy strategy = new Strategy(Command.current(), store);
            source = strategy.source(key, record);
        }
        else {
            source = Source.FIELD;
        }
        Set<TObject> values;
        if(source == Source.RECORD) {
            // @formatter:off
            Map<String, Set<TObject>> stored = timestamp == Time.NONE
                    ? store.select(record)
                    : store.select(record, timestamp);
            values = MoreObjects.firstNonNull(stored.get(key), ImmutableSet.of());
            // @formatter:on
        }
        else if(source == Source.FIELD) {
            // @formatter:off
            values = timestamp == Time.NONE 
                    ? store.select(key, record)
                    : store.select(key, record, timestamp);
            // @formatter:on
        }
        else { // source == Source.INDEX
            // @formatter:off
            values = timestamp == Time.NONE 
                    ? store.gather(key, record)
                    : store.gather(key, record, timestamp);
            // @formatter:on
        }
        return values;
    }

    /**
     * The root node "stop" used for efficient navigation key traversal through
     * the document graph.
     */
    private static final String NAVIGATION_KEYS_GRAPH_ROOT_NODE = "^";

    /**
     * The end node "stop" used for efficient navigation key traversal through
     * the document graph.
     */
    private static final String NAVIGATION_KEYS_GRAPH_END_NODE = "$";

    private Stores() {/* no-init */}

    /**
     * An enumeration that specifies the strategy to use when finding records
     * that match
     * a condition on a navigation key.
     * <p>
     * Navigation keys allow traversing through linked records. This enum
     * provides options
     * for how to efficiently search through these linked structures.
     * </p>
     *
     * @author Jeff Nelson
     */
    enum NavigationKeyFinder {
        /**
         * Automatically select the most efficient traversal strategy based on
         * the characteristics of the data. This will choose between forward
         * traversal, reverse traversal, or ad-hoc indexing depending on the
         * relative sizes of the start and end record sets.
         */
        AUTO,

        /**
         * Use forward traversal, starting with records that have outgoing links
         * on the first stop of the navigation path and following links forward
         * to find matches.
         * <p>
         * This strategy is generally more efficient when there are fewer start
         * records than end records.
         * </p>
         */
        FORWARD_TRAVERSAL,

        /**
         * Use reverse traversal, starting with records that match the final
         * condition and working backwards through the navigation path.
         * <p>
         * This strategy is generally more efficient when there are more start
         * records than end records.
         * </p>
         */
        REVERSE_TRAVERSAL,

        /**
         * Use an ad-hoc index created by browsing all values for the navigation
         * key.
         * <p>
         * This approach creates a temporary index mapping values to records for
         * the entire navigation path, which can be efficient for certain query
         * patterns.
         * </p>
         */
        ADHOC_INDEX
    }

    /**
     * A {@link Map} that imposes a specific iteration order.
     * <p>
     * <strong>NOTE:</strong> This {@link Map} should not be used for general
     * purposes, because it assumes adherence to constraints that are
     * checked/enforced in the methods of this class, but may be legitimately
     * violated in other places (e.g., all the keys specified in the order
     * collection are keys that are held in the data map).
     * </p>
     *
     * @author Jeff Nelson
     */
    static class OrderImposingMap<K, V> extends ForwardingMap<K, V> {

        /**
         * The iteration order of the {@code keys}.
         */
        private final Collection<K> order;

        /**
         * The data that is being ordered
         */
        private final Map<K, V> data;

        /**
         * Construct a new instance.
         * 
         * @param order
         * @param data
         */
        public OrderImposingMap(Collection<K> order, Map<K, V> data) {
            this.order = order;
            this.data = data;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {

                @Override
                public Iterator<Entry<K, V>> iterator() {
                    Iterator<K> it = order.iterator();

                    return new AdHocIterator<Entry<K, V>>() {

                        Entry<K, V> next = null;

                        @Override
                        protected Entry<K, V> findNext() {
                            if(it.hasNext()) {
                                K key = it.next();
                                V value = data.get(key);
                                next = KeyValue.of(key, value);
                            }
                            else {
                                next = null;
                            }
                            return next;
                        }

                    };
                }

                @Override
                public int size() {
                    return data.size();
                }

            };
        }

        @Override
        protected Map<K, V> delegate() {
            return data;
        }

    }

    /**
     * A node on the Graph made up on multiple {@link KeyType#NAVIGATION_KEY
     * navigation keys} that are being selected from a single record.
     * <p>
     * A Graph is used to process {@link KeyType#NAVIGATION_KEY
     * navigation keys} to optimize the number of lookups that are needed to
     * fully process all the selected paths.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static class Node {

        /**
         * Create a root {@link Node} for the graph, starting at {@code record}.
         * 
         * @param record
         * @return the root {@link Node}
         */
        public static Node root(long record) {
            // @foramtter:off
            Node root = new Node(null, NAVIGATION_KEYS_GRAPH_ROOT_NODE,
                    new HashMap<>(),
                    ImmutableSet.of(Convert.javaToThrift(Link.to(record))));
            root.links = ImmutableSet.of(record); // (authorized)
            // @foramtter:on
            return root;
        }

        /**
         * The "stop" along a full {@link KeyType#NAVIGATION_KEY navigation
         * key} path that this {@link Node} represents.
         */
        private final String stop;

        /**
         * The successors {@link Node Nodes}, if any
         */
        @Nullable
        private Map<String, Node> successors;

        /**
         * The values that have been looked up for this {@link Node Node's}
         * {@link #path}.
         */
        private Set<TObject> values;

        /**
         * Any {@link #values} that are {@link Type#LINK Links}; represented as
         * the destination of those Links.
         */
        private Collection<Long> links;

        /**
         * The navigation path up to and including this {@link Node}.
         */
        private final String path;

        /**
         * Construct a new instance.
         * 
         * @param predecessor
         * @param stop
         * @param successors
         * @param values
         */
        private Node(@Nullable Node predecessor, String stop,
                Map<String, Node> successors, Set<TObject> values) {
            StringBuilder path = predecessor != null
                    ? new StringBuilder(predecessor.path)
                    : new StringBuilder();
            if(!stop.equals(NAVIGATION_KEYS_GRAPH_ROOT_NODE)
                    && !stop.equals(NAVIGATION_KEYS_GRAPH_END_NODE)) {
                if(path.length() > 0) {
                    path.append('.');
                }
                path.append(stop);
            }
            this.path = path.toString();
            this.stop = stop;
            this.successors = successors;
            this.values = values;
            this.links = values instanceof ImmutableSet ? ImmutableSet.of()
                    : new ArrayList<>();
        }

        /**
         * Terminate the current path.
         * 
         * @return an end {@link Node}
         */
        public Node end() {
            return next(NAVIGATION_KEYS_GRAPH_END_NODE, ImmutableMap.of(),
                    values);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Node) {
                return ((Node) obj).stop.equals(stop);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return stop.hashCode();
        }

        /**
         * Return the destination records for any {@link #values()} that are
         * {@link Type#LINK Links}.
         * 
         * @return the retrieved values
         */
        public Collection<Long> links() {
            return links;
        }

        /**
         * Create and return a {@link Node} for {@code stop} that is a
         * {@link #successors() successor} to this one.
         * 
         * @param stop
         * @return the next {@link Node}
         */
        public Node next(String stop) {
            return next(stop, new LinkedHashMap<>(), new LinkedHashSet<>());
        }

        /**
         * Create and return a {@link Node} for {@code stop} that is a
         * {@link #successors() successor} to this one.
         * 
         * @param stop
         * @param successors
         * @param values
         * @return the next {@link Node}
         */
        public Node next(String stop, Map<String, Node> successors,
                Set<TObject> values) {
            Node next = this.successors.get(stop);
            if(next == null) {
                next = new Node(this, stop, successors, values);
                this.successors.put(stop, next);
            }
            return next;
        }

        /**
         * Store the {@code values} that have been retrieved for this
         * {@link Node Node's} {@link #stop} within any {@link #links()} of its
         * predecessor.
         * 
         * @param values
         */
        public void store(Set<TObject> values) {
            for (TObject value : values) {
                store(value);
            }
        }

        /**
         * Store the {@code value} that has been retrieved for this
         * {@link Node Node's} {@link #stop} within any {@link #links()} of its
         * predecessor.
         * 
         * @param values
         */
        public void store(TObject value) {
            if(values.add(value) && value.getType() == Type.LINK) {
                links.add(((Link) Convert.thriftToJava(value)).longValue());
            }
        }

        /**
         * Return the successor {@link Node Nodes}.
         * 
         * @return the successor {@link Node Nodes}.
         */
        public Collection<Node> successors() {
            return successors.values();
        }

        @Override
        public String toString() {
            return stop;
        }

        /**
         * Return all the values that have been retrieved for this {@link Node
         * Node's} {@link #stop} from any of the {@link #links()} of its
         * predecessor.
         * 
         * @return the retrieved values
         */
        public Set<TObject> values() {
            return values == null ? ImmutableSet.of() : values;
        }

    }

}
