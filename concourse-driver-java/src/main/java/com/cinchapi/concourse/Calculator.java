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
            TObject result = concourse.$calculate().averageKey(key,
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
            TObject result = concourse.$calculate().averageKeyRecords(key,
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
                    ? concourse.$calculate().averageKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().averageKeyRecordsTime(key,
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
            TObject result = concourse.$calculate().averageKeyCriteria(key,
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
                    ? concourse.$calculate().averageKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().averageKeyCriteriaTime(key,
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
            TObject result = concourse.$calculate().averageKeyRecord(key,
                    record, concourse.creds(), concourse.transaction(),
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
                    ? concourse.$calculate().averageKeyRecordTimestr(key,
                            record, timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().averageKeyRecordTime(key, record,
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
            TObject result = concourse.$calculate().averageKeyCcl(key, ccl,
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
                    ? concourse.$calculate().averageKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().averageKeyCclTime(key, ccl,
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
                    ? concourse.$calculate().averageKeyTimestr(key,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().averageKeyTime(key,
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
            return concourse.$calculate().countKey(key, concourse.creds(),
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
            return concourse.$calculate().countKeyRecords(key,
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
                    ? concourse.$calculate().countKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().countKeyRecordsTime(key,
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
            return concourse.$calculate().countKeyCriteria(key,
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
                    ? concourse.$calculate().countKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().countKeyCriteriaTime(key,
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
            return concourse.$calculate().countKeyRecord(key, record,
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
                    ? concourse.$calculate().countKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().countKeyRecordTime(key, record,
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
            return concourse.$calculate().countKeyCcl(key, ccl,
                    concourse.creds(), concourse.transaction(),
                    concourse.environment());
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
                    ? concourse.$calculate().countKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().countKeyCclTime(key, ccl,
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
                    ? concourse.$calculate().countKeyTimestr(key,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().countKeyTime(key,
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
            TObject result = concourse.$calculate().maxKey(key,
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
     * @return the max of the values
     */
    public Number max(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.$calculate().maxKeyRecords(key,
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
                    ? concourse.$calculate().maxKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().maxKeyRecordsTime(key,
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
            TObject result = concourse.$calculate().maxKeyCriteria(key,
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
                    ? concourse.$calculate().maxKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().maxKeyCriteriaTime(key,
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
            TObject result = concourse.$calculate().maxKeyRecord(key, record,
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
                    ? concourse.$calculate().maxKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().maxKeyRecordTime(key, record,
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
            TObject result = concourse.$calculate().maxKeyCcl(key, ccl,
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
                    ? concourse.$calculate().maxKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().maxKeyCclTime(key, ccl,
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
                    ? concourse.$calculate().maxKeyTimestr(key,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().maxKeyTime(key,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
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
            TObject result = concourse.$calculate().minKey(key,
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
     * @return the min of the values
     */
    public Number min(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.$calculate().minKeyRecords(key,
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
                    ? concourse.$calculate().minKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().minKeyRecordsTime(key,
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
            TObject result = concourse.$calculate().minKeyCriteria(key,
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
                    ? concourse.$calculate().minKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().minKeyCriteriaTime(key,
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
            TObject result = concourse.$calculate().minKeyRecord(key, record,
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
                    ? concourse.$calculate().minKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().minKeyRecordTime(key, record,
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
            TObject result = concourse.$calculate().minKeyCcl(key, ccl,
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
                    ? concourse.$calculate().minKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().minKeyCclTime(key, ccl,
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
                    ? concourse.$calculate().minKeyTimestr(key,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().minKeyTime(key,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
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
            TObject result = concourse.$calculate().sumKey(key,
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
     * @return the sum of the values
     */
    public Number sum(String key, Collection<Long> records) {
        return concourse.execute(() -> {
            TObject result = concourse.$calculate().sumKeyRecords(key,
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
                    ? concourse.$calculate().sumKeyRecordsTimestr(key,
                            Collections.toLongList(records),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().sumKeyRecordsTime(key,
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
            TObject result = concourse.$calculate().sumKeyCriteria(key,
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
                    ? concourse.$calculate().sumKeyCriteriaTimestr(key,
                            Language.translateToThriftCriteria(criteria),
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().sumKeyCriteriaTime(key,
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
            TObject result = concourse.$calculate().sumKeyRecord(key, record,
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
                    ? concourse.$calculate().sumKeyRecordTimestr(key, record,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().sumKeyRecordTime(key, record,
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
            TObject result = concourse.$calculate().sumKeyCcl(key, ccl,
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
                    ? concourse.$calculate().sumKeyCclTimestr(key, ccl,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().sumKeyCclTime(key, ccl,
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
                    ? concourse.$calculate().sumKeyTimestr(key,
                            timestamp.toString(), concourse.creds(),
                            concourse.transaction(), concourse.environment())
                    : concourse.$calculate().sumKeyTime(key,
                            timestamp.getMicros(), concourse.creds(),
                            concourse.transaction(), concourse.environment());
            return (Number) Convert.thriftToJava(result);
        });
    }
}
