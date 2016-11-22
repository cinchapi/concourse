/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.cinchapi.concourse.server.calculate.Calculations;
import com.cinchapi.concourse.server.calculate.KeyCalculation;
import com.cinchapi.concourse.server.calculate.KeyRecordCalculation;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Numbers;

/**
 * A collection of operations that are used in {@link ConcourseServer}.
 * 
 * @author Jeff Nelson
 */
public final class Operations {

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
