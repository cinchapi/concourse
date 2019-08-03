/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
            TObject result = concourse.thrift().averageKey(key,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().averageKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().averageKeyRecordsTime(key,
                            Collections.toLongList(records),
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().averageKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().averageKeyCriteriaTime(key,
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().averageKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().averageKeyRecordTime(key, record,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
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
     * Return the average of all the values at {@code timestamp} for {@code key}
     * in each of the records that match the {@code criteria}.
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().averageKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().averageKeyCclTime(key, ccl,
                            timestamp.getMicros(), concourse.creds(),
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().averageKeyTimestr(
                            key, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().averageKeyTime(key,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the count of all the values stored across {@code key}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @return the count of the values
     */
    public long count(String key) {
        return concourse.execute(() -> {
            return concourse.thrift().countKey(key, concourse.creds(),
                    concourse.transaction(), concourse.environment());
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
     * @return the count of the values
     */
    public long count(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            return concourse.thrift().countKeyRecords(key,
                    Collections.toLongList(records), concourse.creds(),
                    concourse.transaction(), concourse.environment());
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
     * @return the count of the values
     */
    public long count(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.execute(() -> {
            return timestamp.isString()
                    ? concourse.thrift().countKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().countKeyRecordsTime(key,
                            Collections.toLongList(records),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the count of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param {@link Criteria}
     * @return the count of the values
     */
    public long count(String key, Criteria criteria) {
        return concourse.execute(() -> {
            return concourse.thrift().countKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
        });
    }

    /**
     * Return the count of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param {@link Criteria}
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the count of the values
     */
    public long count(String key, Criteria criteria, Timestamp timestamp) {
        return concourse.execute(() -> {
            return timestamp.isString()
                    ? concourse.thrift().countKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().countKeyCriteriaTime(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the count of all the values stored for {@code key} in
     * {@code record}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @return the count of the values
     */
    public long count(String key, long record) {
        return concourse.execute(() -> {
            return concourse.thrift().countKeyRecord(key, record,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
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
     * @return the count of the values
     */
    public long count(String key, long record, Timestamp timestamp) {
        return concourse.execute(() -> {
            return timestamp.isString()
                    ? concourse.thrift().countKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().countKeyRecordTime(key, record,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the count of all the values for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param ccl
     * @return the count of the values
     */
    public long count(String key, String ccl) {
        return concourse.execute(() -> {
            return concourse.thrift().countKeyCcl(key, ccl, concourse.creds(),
                    concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the count of all the values at {@code timestamp} for {@code key}
     * in
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
    public long count(String key, String ccl, Timestamp timestamp) {
        return concourse.execute(() -> {
            return timestamp.isString()
                    ? concourse.thrift().countKeyCclTimestr(
                            key, ccl, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().countKeyCclTime(key, ccl,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the count of all the values stored across {@code key} at
     * {@code timestamp}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the count of the values
     */
    public long count(String key, Timestamp timestamp) {
        return concourse.execute(() -> {
            return timestamp.isString()
                    ? concourse.thrift().countKeyTimestr(
                            key, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().countKeyTime(key,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
        });
    }

    /**
     * Return the max of all the values stored across {@code key}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @return the max of the values
     */
    public Number max(String key) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().maxKey(key, concourse.creds(),
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
     * @return the max of the values
     */
    public Number max(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().maxKeyRecords(key,
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
     * @return the max of the values
     */
    public Number max(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().maxKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().maxKeyRecordsTime(key,
                            Collections.toLongList(records),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param {@link Criteria}
     * @return the max of the values
     */
    public Number max(String key, Criteria criteria) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().maxKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param {@link Criteria}
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the max of the values
     */
    public Number max(String key, Criteria criteria, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().maxKeyCriteriaTimestr(
                            key, Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().maxKeyCriteriaTime(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values stored for {@code key} in
     * {@code record}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @return the max of the values
     */
    public Number max(String key, long record) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().maxKeyRecord(key, record,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
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
     * @return the max of the values
     */
    public Number max(String key, long record, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().maxKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().maxKeyRecordTime(key, record,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param ccl
     * @return the max of the values
     */
    public Number max(String key, String ccl) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().maxKeyCcl(key, ccl,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values at {@code timestamp} for {@code key} in
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
    public Number max(String key, String ccl, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().maxKeyCclTimestr(key,
                            ccl, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().maxKeyCclTime(key, ccl,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the max of all the values stored across {@code key} at
     * {@code timestamp}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the max of the values
     */
    public Number max(String key, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().maxKeyTimestr(
                            key, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().maxKeyTime(key, timestamp.getMicros(),
                            concourse.creds(), concourse.transaction(),
                            concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values stored across {@code key}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @return the min of the values
     */
    public Number min(String key) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().minKey(key, concourse.creds(),
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
     * @return the min of the values
     */
    public Number min(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().minKeyRecords(key,
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
     * @return the min of the values
     */
    public Number min(String key, Collection<Long> records,
            Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().minKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().minKeyRecordsTime(key,
                            Collections.toLongList(records),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key
     * @param {@link Criteria}
     * @return the min of the values
     */
    public Number min(String key, Criteria criteria) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().minKeyCriteria(key,
                    Language.translateToThriftCriteria(criteria),
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values for {@code key} in
     * each of the records that match the {@link Criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param {@link Criteria}
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the min of the values
     */
    public Number min(String key, Criteria criteria, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().minKeyCriteriaTimestr(
                            key, Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().minKeyCriteriaTime(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values stored for {@code key} in
     * {@code record}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param record the record id
     * @return the min of the values
     */
    public Number min(String key, long record) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().minKeyRecord(key, record,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
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
     * @return the min of the values
     */
    public Number min(String key, long record, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().minKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().minKeyRecordTime(key, record,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values for {@code key} in
     * each of the records that match the {@code criteria}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key the field name
     * @param ccl
     * @return the min of the values
     */
    public Number min(String key, String ccl) {
        return concourse.execute(() -> {
            TObject result = concourse.thrift().minKeyCcl(key, ccl,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values at {@code timestamp} for {@code key} in
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
    public Number min(String key, String ccl, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().minKeyCclTimestr(key,
                            ccl, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().minKeyCclTime(key, ccl,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }

    /**
     * Return the min of all the values stored across {@code key} at
     * {@code timestamp}.
     * <p>
     * This method assumes that all the values are numeric. An exception will be
     * thrown if any non-numeric values are encountered.
     * </p>
     * 
     * @param key a field name
     * @param timestamp the {@link Timestamp} at which the values are selected
     * @return the min of the values
     */
    public Number min(String key, Timestamp timestamp) {
        return concourse.execute(() -> {
            TObject result = timestamp.isString()
                    ? concourse.thrift().minKeyTimestr(
                            key, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().minKeyTime(key, timestamp.getMicros(),
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().sumKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().sumKeyRecordsTime(key,
                            Collections.toLongList(records),
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().sumKeyCriteriaTimestr(
                            key, Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().sumKeyCriteriaTime(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().sumKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().sumKeyRecordTime(key, record,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().sumKeyCclTimestr(key,
                            ccl, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().sumKeyCclTime(key, ccl,
                            timestamp.getMicros(), concourse.creds(),
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
            TObject result = timestamp.isString()
                    ? concourse.thrift().sumKeyTimestr(
                            key, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.thrift().sumKeyTime(key, timestamp.getMicros(),
                            concourse.creds(), concourse.transaction(),
                            concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }
}
