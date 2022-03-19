/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.IndexFunction;
import com.cinchapi.ccl.type.function.KeyConditionFunction;
import com.cinchapi.ccl.type.function.KeyRecordsFunction;
import com.cinchapi.ccl.type.function.TemporalFunction;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Constants;
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
import com.cinchapi.concourse.validate.Keys;
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
     * satisify {@code operator} in relation to the specified {@code values} at
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
        if(Keys.isNavigationKey(key)) {
            Map<TObject, Set<Long>> index = timestamp == Time.NONE
                    ? browse(store, key)
                    : browse(store, key, timestamp);
            Set<Long> records = index.entrySet().stream()
                    .filter(e -> e.getKey().is(operator, values))
                    .map(e -> e.getValue()).flatMap(Set::stream)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return records;
        }
        else if(Keys.isFunctionKey(key)) {
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
     * @param timestamp
     * 
     * @return the values stored for {@code key} in {@code record} according to
     *         the {@code store}
     */
    public static Set<TObject> select(Store store, String key, long record) {
        return select(store, key, record, Time.NONE);
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

    private Stores() {/* no-init */}

}
