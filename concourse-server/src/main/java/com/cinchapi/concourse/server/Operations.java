/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.lang.ConjunctionSymbol;
import com.cinchapi.concourse.lang.Expression;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.lang.Parser;
import com.cinchapi.concourse.lang.PostfixNotationSymbol;
import com.cinchapi.concourse.lang.Symbol;
import com.cinchapi.concourse.server.ConcourseServer.DeferredWrite;
import com.cinchapi.concourse.server.calculate.Calculations;
import com.cinchapi.concourse.server.calculate.KeyCalculation;
import com.cinchapi.concourse.server.calculate.KeyRecordCalculation;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TSymbol;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.DataServices;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TSets;
import com.cinchapi.concourse.util.Convert.ResolvableLink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
final class Operations {

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
     * Parse the thrift represented {@code criteria} into an {@link Queue} of
     * {@link PostfixNotationSymbol postfix notation symbols} that can be used
     * within the {@link #findAtomic(Queue, Deque, AtomicOperation)} method.
     *
     * @param criteria
     * @return
     */
    public static Queue<PostfixNotationSymbol> convertCriteriaToQueue(
            TCriteria criteria) {
        List<Symbol> symbols = Lists.newArrayList();
        for (TSymbol tsymbol : criteria.getSymbols()) {
            symbols.add(Language.translateFromThriftSymbol(tsymbol));
        }
        Queue<PostfixNotationSymbol> queue = Parser.toPostfixNotation(symbols);
        return queue;
    }

    /**
     * Do the work necessary to complete a complex find operation based on the
     * {@code queue} of symbols.
     * <p>
     * This method does not return a value. If you need to perform a complex
     * find using an {@link AtomicOperation} and immediately get the results,
     * then you should pass an empty stack into this method and then pop the
     * results after the method executes.
     *
     * <pre>
     * Queue&lt;PostfixNotationSymbol&gt; queue = Parser.toPostfixNotation(ccl);
     * Deque&lt;Set&lt;Long&gt;&gt; stack = new ArrayDeque&lt;Set&lt;Long&gt;&gt;();
     * findAtomic(queue, stack, atomic)
     * Set&lt;Long&gt; matches = stack.pop();
     * </pre>
     *
     * </p>
     *
     * @param queue - The criteria/ccl represented as a queue in postfix
     *            notation. Use {@link Parser#toPostfixNotation(List)} or
     *            {@link Parser#toPostfixNotation(String)} or
     *            {@link #Operations.convertCriteriaToQueue(TCriteria)} to get
     *            this value.
     *            This is modified in place.
     * @param stack - A stack that contains Sets of records that match the
     *            corresponding criteria branches in the {@code queue}. This is
     *            modified in-place.
     * @param atomic - The atomic operation
     */
    public static void findAtomic(Queue<PostfixNotationSymbol> queue,
            Deque<Set<Long>> stack, AtomicOperation atomic) {
        // NOTE: there is room to do some query planning/optimization by going
        // through the pfn and plotting an Abstract Syntax Tree and looking for
        // the optimal routes to start with
        Preconditions.checkArgument(stack.isEmpty());
        for (PostfixNotationSymbol symbol : queue) {
            if(symbol == ConjunctionSymbol.AND) {
                stack.push(TSets.intersection(stack.pop(), stack.pop()));
            }
            else if(symbol == ConjunctionSymbol.OR) {
                stack.push(TSets.union(stack.pop(), stack.pop()));
            }
            else if(symbol instanceof Expression) {
                Expression exp = (Expression) symbol;
                if(exp.getKeyRaw()
                        .equals(Constants.JSON_RESERVED_IDENTIFIER_NAME)) {
                    Set<Long> ids;
                    if(exp.getOperatorRaw() == Operator.EQUALS) {
                        ids = Sets.newTreeSet();
                        for (TObject tObj : exp.getValuesRaw()) {
                            ids.add(((Number) Convert.thriftToJava(tObj))
                                    .longValue());
                        }
                        stack.push(ids);
                    }
                    else if(exp.getOperatorRaw() == Operator.NOT_EQUALS) {
                        ids = atomic.getAllRecords();
                        for (TObject tObj : exp.getValuesRaw()) {
                            ids.remove(((Number) Convert.thriftToJava(tObj))
                                    .longValue());
                        }
                        stack.push(ids);
                    }
                    else {
                        throw new IllegalArgumentException(
                                "Cannot query on record id using "
                                        + exp.getOperatorRaw());
                    }
                }
                else {
                    stack.push(exp.getTimestampRaw() == 0
                            ? atomic.find(exp.getKeyRaw(), exp.getOperatorRaw(),
                                    exp.getValuesRaw())
                            : atomic.find(exp.getTimestampRaw(),
                                    exp.getKeyRaw(), exp.getOperatorRaw(),
                                    exp.getValuesRaw()));
                }
            }
            else {
                // If we reach here, then the conversion to postfix notation
                // failed :-/
                throw new IllegalStateException();
            }
        }
    }

    /**
     * Atomically insert a list of {@link DeferredWrite deferred writes}. This
     * method should only be called after all necessary calls to
     * {@link #insertAtomic(Multimap, long, AtomicOperation, List)} have been
     * made.
     *
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
                Queue<PostfixNotationSymbol> queue = Parser
                        .toPostfixNotation(rlink.getCcl());
                Deque<Set<Long>> stack = new ArrayDeque<Set<Long>>();
                Operations.findAtomic(queue, stack, atomic);
                Set<Long> targets = stack.pop();
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
     * Find data matching the criteria described by the {@code queue} or insert
     * each of the {@code objects} into a new record. Either way, place the
     * records that match the criteria or that contain the inserted data into
     * {@code records}.
     *
     * @param records - the collection that holds the records that either match
     *            the criteria or hold the inserted objects.
     * @param objects - a list of Multimaps, each of which containing data to
     *            insert into a distinct record. Get this using the
     *            {@link Convert#anyJsonToJava(String)} method.
     * @param queue - the parsed criteria attained from
     *            {@link #Operations.convertCriteriaToQueue(TCriteria)} or
     *            {@link Parser#toPostfixNotation(String)}.
     * @param stack - a stack (usually empty) that is used while processing the
     *            query
     * @param atomic - the atomic operation through which all operations are
     *            conducted
     */
    public static void findOrInsertAtomic(Set<Long> records,
            List<Multimap<String, Object>> objects,
            Queue<PostfixNotationSymbol> queue, Deque<Set<Long>> stack,
            AtomicOperation atomic) {
        findAtomic(queue, stack, atomic);
        records.addAll(stack.pop());
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
                    ? store.select(record) : store.select(record, timestamp);
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
     * Use the provided {@code atomic} operation to add each of the values
     * stored across {@code key} at {@code timestamp} to the running
     * {@code sum}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to use
     * @return the new running sum
     */
    public static Number avgKeyAtomic(String key, long timestamp,
            AtomicOperation atomic) {
        Map<TObject, Set<Long>> data = timestamp == Time.NONE
                ? atomic.browse(key) : atomic.browse(key, timestamp);
        Number avg = 0;
        int count = 0;
        for (Entry<TObject, Set<Long>> entry : data.entrySet()) {
            TObject tobject = entry.getKey();
            Set<Long> records = entry.getValue();
            Object value = Convert.thriftToJava(tobject);
            Calculations.checkCalculatable(value);
            Number number = (Number) value;
            number = Numbers.multiply(number, records.size());
            count += records.size();
            avg = Numbers.incrementalAverage(avg, number, count);
        }
        return avg;
    }

    /**
     * Use the provided {@code atomic} operation to add each of the values in
     * {@code key}/{@code record} at {@code timestamp} to the running
     * {@code sum}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to use
     * @return the new running sum
     */
    public static Number avgKeyRecordAtomic(String key, long record,
            long timestamp, AtomicOperation atomic) {
        Set<TObject> values = timestamp == Time.NONE
                ? atomic.select(key, record)
                : atomic.select(key, record, timestamp);
        Number sum = 0;
        for (TObject value : values) {
            Object object = Convert.thriftToJava(value);
            Calculations.checkCalculatable(object);
            Number number = (Number) object;
            sum = Numbers.add(sum, number);
        }
        return Numbers.divide(sum, values.size());
    }

    /**
     * Use the provided {@code atomic} operation to add each of the values
     * stored for the
     * {@code key} in each of the {@code records} at {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to use
     * @return the new running sum
     */
    public static Number avgKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, AtomicOperation atomic) {
        int count = 0;
        Number avg = 0;
        for (long record : records) {
            Set<TObject> values = timestamp == Time.NONE
                    ? atomic.select(key, record)
                    : atomic.select(key, record, timestamp);
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
     * Join the {@link AtomicOperation atomic} operation to compute the sum
     * across the {@code key} at {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to join
     * @return the sum
     */
    public static Number sumKeyAtomic(String key, long timestamp,
            AtomicOperation atomic) {
        return calculateKeyAtomic(key, timestamp, 0, atomic,
                Calculations.sumKey());
    }

    /**
     * Join the {@link AtomicOperation atomic} operation to compute the sum
     * across all the values stored for {@code key} in {@code record} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param record the record id
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to join
     * @return the sum
     */
    public static Number sumKeyRecordAtomic(String key, long record,
            long timestamp, AtomicOperation atomic) {
        return calculateKeyRecordAtomic(key, record, timestamp, 0, atomic,
                Calculations.sumKeyRecord());
    }

    /**
     * Join the {@link AtomicOperation atomic} operation to compute the sum
     * across all the values stored for {@code key} in each of the
     * {@code records} at {@code timestamp}.
     * 
     * @param key the field name
     * @param records the record ids
     * @param timestamp the selection timestamp
     * @param atomic the {@link AtomicOperation} to join
     * @return the sum
     */
    public static Number sumKeyRecordsAtomic(String key,
            Collection<Long> records, long timestamp, AtomicOperation atomic) {
        Number sum = 0;
        for (long record : records) {
            sum = calculateKeyRecordAtomic(key, record, timestamp, sum, atomic,
                    Calculations.sumKeyRecord());
        }
        return sum;
    }

    /**
     * Use the provided {@link AtomicOperation atomic} operation to perform the
     * specified {@code calculation} across the {@code key} at
     * {@code timestamp}.
     * 
     * @param key the field name
     * @param timestamp the selection timestamp
     * @param result the running result
     * @param atomic the {@link AtomicOperation} to use
     * @param calculation the calculation logic
     * @return the result after applying the {@code calculation}
     */
    private static Number calculateKeyAtomic(String key, long timestamp,
            Number result, AtomicOperation atomic, KeyCalculation calculation) {
        Map<TObject, Set<Long>> data = timestamp == Time.NONE
                ? atomic.browse(key) : atomic.browse(key, timestamp);
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
     * @param atomic the {@link AtomicOperation} to use
     * @param calculation the calculation logic
     * @return the result after appltying the {@code calculation}
     */
    private static Number calculateKeyRecordAtomic(String key, long record,
            long timestamp, Number result, AtomicOperation atomic,
            KeyRecordCalculation calculation) {
        Set<TObject> values = timestamp == Time.NONE
                ? atomic.select(key, record)
                : atomic.select(key, record, timestamp);
        for (TObject tobject : values) {
            Object value = Convert.thriftToJava(tobject);
            Calculations.checkCalculatable(value);
            result = calculation.calculate(result, (Number) value);
        }
        return result;
    }

    private Operations() {/* no-op */}

}
