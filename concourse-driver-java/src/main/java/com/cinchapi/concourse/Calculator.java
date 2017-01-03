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
package com.cinchapi.concourse;

import java.util.Collection;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.Language;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Collections;
import com.cinchapi.concourse.util.Convert;
import com.google.common.base.Preconditions;

/**
 * The interface for Concourse's aggregation and calculation abilities.
 * 
 * @author Jeff Nelson
 */
public class Calculator {

    /**
     * The parent driver that contains the connection to thrift.
     */
    private final ConcourseThriftDriver concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    Calculator(Concourse concourse) {
        Preconditions.checkArgument(concourse instanceof ConcourseThriftDriver);
        this.concourse = (ConcourseThriftDriver) concourse;
    }

    /**
     * Return the sum of all the values stored for {@code key} in
     * {@code record}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @return the sum of the values
     */
    public Number sum(String key, long record) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyRecord(key, record,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values stored across {@code key}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @return the sum of the values
     */
    public Number sum(String key) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKey(key, concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values stored across {@code key} at
     * {@code timestamp}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the sum of the values
     */
    public Number sum(String key, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyTime(key,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the sum of the values
     */
    public Number sum(String key, long record, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyRecordTime(key, record,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param records
     * @return the sum of the values
     */
    public Number sum(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyRecords(key,
                    Collections.toLongList(records), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param records
     * @param timestamp
     * @return the sum of the values
     */
    public Number sum(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyRecordsTime(key,
                    Collections.toLongList(records), timestamp.getMicros(),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param {@link Criteria}
     * @return the sum of the values
     */
    public Number sum(String key, Criteria criteria) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param ccl
     * @return the sum of the values
     */
    public Number sum(String key, String ccl) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyCcl(key, ccl,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values at {@code timestamp} for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param criteria a criteria written using the Concourse Criteria Language
     *            (CCL)
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the sum of the values
     */
    public Number sum(String key, String ccl, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyCclTime(key, ccl,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the sum of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param {@link Criteria}
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the sum of the values
     */
    public Number sum(String key, Criteria criteria, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().sumKeyCriteriaTime(key,
                    Language.translateToThriftCriteria(criteria),
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }
    
    /**
     * Return the average of all the values stored for {@code key} in
     * {@code record}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @return the average of the values
     */
    public Number average(String key, long record) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyRecord(key, record,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values stored across {@code key}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @return the average of the values
     */
    public Number average(String key) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKey(key, concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values stored across {@code key} at
     * {@code timestamp}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the average of the values
     */
    public Number average(String key, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyTime(key,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param record
     * @param timestamp
     * @return the average of the values
     */
    public Number average(String key, long record, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyRecordTime(key, record,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param records
     * @return the average of the values
     */
    public Number average(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyRecords(key,
                    Collections.toLongList(records), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param records
     * @param timestamp
     * @return the average of the values
     */
    public Number average(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyRecordsTime(key,
                    Collections.toLongList(records), timestamp.getMicros(),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param {@link Criteria}
     * @return the average of the values
     */
    public Number average(String key, Criteria criteria) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param ccl
     * @return the average of the values
     */
    public Number average(String key, String ccl) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyCcl(key, ccl,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values at {@code timestamp} for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param criteria a criteria written using the Concourse Criteria Language
     *            (CCL)
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the average of the values
     */
    public Number average(String key, String ccl, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyCclTime(key, ccl,
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the average of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param {@link Criteria}
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the average of the values
     */
    public Number average(String key, Criteria criteria, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().averageKeyCriteriaTime(key,
                    Language.translateToThriftCriteria(criteria),
                    timestamp.getMicros(), concourse.creds(),
                    concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }
}
