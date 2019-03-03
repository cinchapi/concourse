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
package com.cinchapi.concourse.server.plugin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A modified version of {@link ConcourseService} that maintains client state
 * internally and therefore doesn't require the presentation of state variables
 * (e.g. AccessToken, TransactionToken and environment) as parameters to any
 * methods.
 */
abstract class StatefulConcourseService {

    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TObjects. For convenience, a StatefulConcourseService
     * accepts generic objects for those parameters and we must keep track here
     * so it is known what must be translated into a TObject for proper routing
     * in ConcourseServer.
     */
    protected static Multimap<String, Integer> VALUE_TRANSFORM = HashMultimap
            .create();

    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TCriteria objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TCriteria for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> CRITERIA_TRANSFORM = HashMultimap
            .create();

    /**
     * A collection of Thrift methods that have a return value that contains
     * a TObject. For convenience, a StatefulConcourseService will return
     * generic objects and we must keep track here so it is known what must be
     * translated from a TObject.
     */
    protected static Set<String> RETURN_TRANSFORM = new HashSet<String>();
    static {

        VALUE_TRANSFORM.put("addKeyValue", 1);

        VALUE_TRANSFORM.put("addKeyValueRecord", 1);

        VALUE_TRANSFORM.put("addKeyValueRecords", 1);

        RETURN_TRANSFORM.add("browseKey");

        RETURN_TRANSFORM.add("browseKeys");

        RETURN_TRANSFORM.add("browseKeyTime");

        RETURN_TRANSFORM.add("browseKeyTimestr");

        RETURN_TRANSFORM.add("browseKeysTime");

        RETURN_TRANSFORM.add("browseKeysTimestr");

        RETURN_TRANSFORM.add("chronologizeKeyRecord");

        RETURN_TRANSFORM.add("chronologizeKeyRecordStart");

        RETURN_TRANSFORM.add("chronologizeKeyRecordStartstr");

        RETURN_TRANSFORM.add("chronologizeKeyRecordStartEnd");

        RETURN_TRANSFORM.add("chronologizeKeyRecordStartstrEndstr");

        RETURN_TRANSFORM.add("diffRecordStart");

        RETURN_TRANSFORM.add("diffRecordStartstr");

        RETURN_TRANSFORM.add("diffRecordStartEnd");

        RETURN_TRANSFORM.add("diffRecordStartstrEndstr");

        RETURN_TRANSFORM.add("diffKeyRecordStart");

        RETURN_TRANSFORM.add("diffKeyRecordStartstr");

        RETURN_TRANSFORM.add("diffKeyRecordStartEnd");

        RETURN_TRANSFORM.add("diffKeyRecordStartstrEndstr");

        RETURN_TRANSFORM.add("diffKeyStart");

        RETURN_TRANSFORM.add("diffKeyStartstr");

        RETURN_TRANSFORM.add("diffKeyStartEnd");

        RETURN_TRANSFORM.add("diffKeyStartstrEndstr");

        VALUE_TRANSFORM.put("removeKeyValueRecord", 1);

        VALUE_TRANSFORM.put("removeKeyValueRecords", 1);

        VALUE_TRANSFORM.put("setKeyValueRecord", 1);

        VALUE_TRANSFORM.put("setKeyValue", 1);

        VALUE_TRANSFORM.put("setKeyValueRecords", 1);

        VALUE_TRANSFORM.put("reconcileKeyRecordValues", 2);

        RETURN_TRANSFORM.add("selectRecord");

        RETURN_TRANSFORM.add("selectRecords");

        RETURN_TRANSFORM.add("selectRecordTime");

        RETURN_TRANSFORM.add("selectRecordTimestr");

        RETURN_TRANSFORM.add("selectRecordsTime");

        RETURN_TRANSFORM.add("selectRecordsTimestr");

        RETURN_TRANSFORM.add("selectKeyRecord");

        RETURN_TRANSFORM.add("selectKeyRecordTime");

        RETURN_TRANSFORM.add("selectKeyRecordTimestr");

        RETURN_TRANSFORM.add("selectKeysRecord");

        RETURN_TRANSFORM.add("selectKeysRecordTime");

        RETURN_TRANSFORM.add("selectKeysRecordTimestr");

        RETURN_TRANSFORM.add("selectKeysRecords");

        RETURN_TRANSFORM.add("selectKeyRecords");

        RETURN_TRANSFORM.add("selectKeyRecordsTime");

        RETURN_TRANSFORM.add("selectKeyRecordsTimestr");

        RETURN_TRANSFORM.add("selectKeysRecordsTime");

        RETURN_TRANSFORM.add("selectKeysRecordsTimestr");

        RETURN_TRANSFORM.add("selectCriteria");

        CRITERIA_TRANSFORM.put("selectCriteria", 0);

        RETURN_TRANSFORM.add("selectCcl");

        RETURN_TRANSFORM.add("selectCriteriaTime");

        CRITERIA_TRANSFORM.put("selectCriteriaTime", 0);

        RETURN_TRANSFORM.add("selectCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectCriteriaTimestr", 0);

        RETURN_TRANSFORM.add("selectCclTime");

        RETURN_TRANSFORM.add("selectCclTimestr");

        RETURN_TRANSFORM.add("selectKeyCriteria");

        CRITERIA_TRANSFORM.put("selectKeyCriteria", 1);

        RETURN_TRANSFORM.add("selectKeyCcl");

        RETURN_TRANSFORM.add("selectKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("selectKeyCclTime");

        RETURN_TRANSFORM.add("selectKeyCclTimestr");

        RETURN_TRANSFORM.add("selectKeysCriteria");

        CRITERIA_TRANSFORM.put("selectKeysCriteria", 1);

        RETURN_TRANSFORM.add("selectKeysCcl");

        RETURN_TRANSFORM.add("selectKeysCriteriaTime");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTime", 1);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("selectKeysCclTime");

        RETURN_TRANSFORM.add("selectKeysCclTimestr");

        RETURN_TRANSFORM.add("getKeyRecord");

        RETURN_TRANSFORM.add("getKeyRecordTime");

        RETURN_TRANSFORM.add("getKeyRecordTimestr");

        RETURN_TRANSFORM.add("getKeysRecord");

        RETURN_TRANSFORM.add("getKeysRecordTime");

        RETURN_TRANSFORM.add("getKeysRecordTimestr");

        RETURN_TRANSFORM.add("getKeysRecords");

        RETURN_TRANSFORM.add("getKeyRecords");

        RETURN_TRANSFORM.add("getKeyRecordsTime");

        RETURN_TRANSFORM.add("getKeyRecordsTimestr");

        RETURN_TRANSFORM.add("getKeysRecordsTime");

        RETURN_TRANSFORM.add("getKeysRecordsTimestr");

        RETURN_TRANSFORM.add("getKeyCriteria");

        CRITERIA_TRANSFORM.put("getKeyCriteria", 1);

        RETURN_TRANSFORM.add("getCriteria");

        CRITERIA_TRANSFORM.put("getCriteria", 0);

        RETURN_TRANSFORM.add("getCcl");

        RETURN_TRANSFORM.add("getCriteriaTime");

        CRITERIA_TRANSFORM.put("getCriteriaTime", 0);

        RETURN_TRANSFORM.add("getCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getCriteriaTimestr", 0);

        RETURN_TRANSFORM.add("getCclTime");

        RETURN_TRANSFORM.add("getCclTimestr");

        RETURN_TRANSFORM.add("getKeyCcl");

        RETURN_TRANSFORM.add("getKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("getKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("getKeyCclTime");

        RETURN_TRANSFORM.add("getKeyCclTimestr");

        RETURN_TRANSFORM.add("getKeysCriteria");

        CRITERIA_TRANSFORM.put("getKeysCriteria", 1);

        RETURN_TRANSFORM.add("getKeysCcl");

        RETURN_TRANSFORM.add("getKeysCriteriaTime");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTime", 1);

        RETURN_TRANSFORM.add("getKeysCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("getKeysCclTime");

        RETURN_TRANSFORM.add("getKeysCclTimestr");

        VALUE_TRANSFORM.put("verifyKeyValueRecord", 1);

        VALUE_TRANSFORM.put("verifyKeyValueRecordTime", 1);

        VALUE_TRANSFORM.put("verifyKeyValueRecordTimestr", 1);

        CRITERIA_TRANSFORM.put("findCriteria", 0);

        VALUE_TRANSFORM.put("findKeyOperatorValues", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTime", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimestr", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValues", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTime", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimestr", 2);

        VALUE_TRANSFORM.put("verifyAndSwap", 1);

        VALUE_TRANSFORM.put("verifyAndSwap", 3);

        VALUE_TRANSFORM.put("verifyOrSet", 1);

        VALUE_TRANSFORM.put("findOrAddKeyValue", 1);

        CRITERIA_TRANSFORM.put("findOrInsertCriteriaJson", 0);

        RETURN_TRANSFORM.add("sumKeyRecord");

        RETURN_TRANSFORM.add("sumKeyRecordTime");

        RETURN_TRANSFORM.add("sumKeyRecordTimestr");

        RETURN_TRANSFORM.add("sumKeyRecords");

        RETURN_TRANSFORM.add("sumKeyRecordsTime");

        RETURN_TRANSFORM.add("sumKeyRecordsTimestr");

        RETURN_TRANSFORM.add("sumKey");

        RETURN_TRANSFORM.add("sumKeyTime");

        RETURN_TRANSFORM.add("sumKeyTimestr");

        RETURN_TRANSFORM.add("sumKeyCriteria");

        CRITERIA_TRANSFORM.put("sumKeyCriteria", 1);

        RETURN_TRANSFORM.add("sumKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("sumKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("sumKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("sumKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("sumKeyCcl");

        RETURN_TRANSFORM.add("sumKeyCclTime");

        RETURN_TRANSFORM.add("sumKeyCclTimestr");

        RETURN_TRANSFORM.add("averageKeyRecord");

        RETURN_TRANSFORM.add("averageKeyRecordTime");

        RETURN_TRANSFORM.add("averageKeyRecordTimestr");

        RETURN_TRANSFORM.add("averageKeyRecords");

        RETURN_TRANSFORM.add("averageKeyRecordsTime");

        RETURN_TRANSFORM.add("averageKeyRecordsTimestr");

        RETURN_TRANSFORM.add("averageKey");

        RETURN_TRANSFORM.add("averageKeyTime");

        RETURN_TRANSFORM.add("averageKeyTimestr");

        RETURN_TRANSFORM.add("averageKeyCriteria");

        CRITERIA_TRANSFORM.put("averageKeyCriteria", 1);

        RETURN_TRANSFORM.add("averageKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("averageKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("averageKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("averageKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("averageKeyCcl");

        RETURN_TRANSFORM.add("averageKeyCclTime");

        RETURN_TRANSFORM.add("averageKeyCclTimestr");

        CRITERIA_TRANSFORM.put("countKeyCriteria", 1);

        CRITERIA_TRANSFORM.put("countKeyCriteriaTime", 1);

        CRITERIA_TRANSFORM.put("countKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("maxKeyRecord");

        RETURN_TRANSFORM.add("maxKeyRecordTime");

        RETURN_TRANSFORM.add("maxKeyRecordTimestr");

        RETURN_TRANSFORM.add("maxKeyRecords");

        RETURN_TRANSFORM.add("maxKeyRecordsTime");

        RETURN_TRANSFORM.add("maxKeyRecordsTimestr");

        RETURN_TRANSFORM.add("maxKeyCriteria");

        CRITERIA_TRANSFORM.put("maxKeyCriteria", 1);

        RETURN_TRANSFORM.add("maxKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("maxKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("maxKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("maxKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("maxKeyCcl");

        RETURN_TRANSFORM.add("maxKeyCclTime");

        RETURN_TRANSFORM.add("maxKeyCclTimestr");

        RETURN_TRANSFORM.add("maxKey");

        RETURN_TRANSFORM.add("maxKeyTime");

        RETURN_TRANSFORM.add("maxKeyTimestr");

        RETURN_TRANSFORM.add("minKeyRecord");

        RETURN_TRANSFORM.add("minKeyRecordTime");

        RETURN_TRANSFORM.add("minKeyRecordTimestr");

        RETURN_TRANSFORM.add("minKey");

        RETURN_TRANSFORM.add("minKeyRecordsTime");

        RETURN_TRANSFORM.add("minKeyRecordsTimestr");

        RETURN_TRANSFORM.add("minKeyCriteria");

        CRITERIA_TRANSFORM.put("minKeyCriteria", 1);

        RETURN_TRANSFORM.add("minKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("minKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("minKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("minKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("minKeyCcl");

        RETURN_TRANSFORM.add("minKeyCclTime");

        RETURN_TRANSFORM.add("minKeyCclTimestr");

        RETURN_TRANSFORM.add("minKeyTime");

        RETURN_TRANSFORM.add("minKeyTimestr");

        RETURN_TRANSFORM.add("minKeyRecords");

        RETURN_TRANSFORM.add("navigateKeyRecord");

        RETURN_TRANSFORM.add("navigateKeyRecordTime");

        RETURN_TRANSFORM.add("navigateKeyRecordTimestr");

        RETURN_TRANSFORM.add("navigateKeysRecord");

        RETURN_TRANSFORM.add("navigateKeysRecordTime");

        RETURN_TRANSFORM.add("navigateKeysRecordTimestr");

        RETURN_TRANSFORM.add("navigateKeysRecords");

        RETURN_TRANSFORM.add("navigateKeyRecords");

        RETURN_TRANSFORM.add("navigateKeyRecordsTime");

        RETURN_TRANSFORM.add("navigateKeyRecordsTimestr");

        RETURN_TRANSFORM.add("navigateKeysRecordsTime");

        RETURN_TRANSFORM.add("navigateKeysRecordsTimestr");

        RETURN_TRANSFORM.add("navigateKeyCcl");

        RETURN_TRANSFORM.add("navigateKeyCclTime");

        RETURN_TRANSFORM.add("navigateKeyCclTimestr");

        RETURN_TRANSFORM.add("navigateKeysCcl");

        RETURN_TRANSFORM.add("navigateKeysCclTime");

        RETURN_TRANSFORM.add("navigateKeysCclTimestr");

        RETURN_TRANSFORM.add("navigateKeyCriteria");

        CRITERIA_TRANSFORM.put("navigateKeyCriteria", 1);

        RETURN_TRANSFORM.add("navigateKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("navigateKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("navigateKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("navigateKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("navigateKeysCriteria");

        CRITERIA_TRANSFORM.put("navigateKeysCriteria", 1);

        RETURN_TRANSFORM.add("navigateKeysCriteriaTime");

        CRITERIA_TRANSFORM.put("navigateKeysCriteriaTime", 1);

        RETURN_TRANSFORM.add("navigateKeysCriteriaTimestr");

        CRITERIA_TRANSFORM.put("navigateKeysCriteriaTimestr", 1);

    }

    public long addKeyValue(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    public boolean addKeyValueRecord(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Boolean> addKeyValueRecords(String key, Object value,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditRecordStart(long record, long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditRecordStartstr(long record, String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditRecordStartEnd(long record, long start,
            long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditRecordStartstrEndstr(long record,
            String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditKeyRecordStart(String key, long record,
            long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditKeyRecordStartstr(String key, long record,
            String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditKeyRecordStartEnd(String key, long record,
            long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, String> auditKeyRecordStartstrEndstr(String key,
            long record, String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Set<Long>> browseKey(String key) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Object, Set<Long>>> browseKeys(List<String> keys) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Set<Long>> browseKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Set<Long>> browseKeyTimestr(String key,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Object, Set<Long>>> browseKeysTime(List<String> keys,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Object, Set<Long>>> browseKeysTimestr(
            List<String> keys, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> chronologizeKeyRecord(String key,
            long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> chronologizeKeyRecordStart(String key,
            long record, long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> chronologizeKeyRecordStartstr(String key,
            long record, String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> chronologizeKeyRecordStartEnd(String key,
            long record, long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> chronologizeKeyRecordStartstrEndstr(
            String key, long record, String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public void clearRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public void clearRecords(List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public void clearKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public void clearKeysRecord(List<String> keys, long record) {
        throw new UnsupportedOperationException();
    }

    public void clearKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public void clearKeysRecords(List<String> keys, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Set<String> describe() {
        throw new UnsupportedOperationException();
    }

    public Set<String> describeTime(long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<String> describeTimestr(String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<String> describeRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public Set<String> describeRecordTime(long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<String> describeRecordTimestr(long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<String>> describeRecords(List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<String>> describeRecordsTime(List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<String>> describeRecordsTimestr(List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Diff, Set<Object>>> diffRecordStart(long record,
            long start) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Diff, Set<Object>>> diffRecordStartstr(long record,
            String start) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Diff, Set<Object>>> diffRecordStartEnd(long record,
            long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<Diff, Set<Object>>> diffRecordStartstrEndstr(
            long record, String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Diff, Set<Object>> diffKeyRecordStart(String key, long record,
            long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Diff, Set<Object>> diffKeyRecordStartstr(String key, long record,
            String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Diff, Set<Object>> diffKeyRecordStartEnd(String key, long record,
            long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Diff, Set<Object>> diffKeyRecordStartstrEndstr(String key,
            long record, String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Map<Diff, Set<Long>>> diffKeyStart(String key,
            long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Map<Diff, Set<Long>>> diffKeyStartstr(String key,
            String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Map<Diff, Set<Long>>> diffKeyStartEnd(String key,
            long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Object, Map<Diff, Set<Long>>> diffKeyStartstrEndstr(String key,
            String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public ComplexTObject invokePlugin(String id, String method,
            List<ComplexTObject> params) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> insertJson(String json) {
        throw new UnsupportedOperationException();
    }

    public boolean insertJsonRecord(String json, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Boolean> insertJsonRecords(String json,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public boolean removeKeyValueRecord(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Boolean> removeKeyValueRecords(String key, Object value,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public void setKeyValueRecord(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    public long setKeyValue(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    public void setKeyValueRecords(String key, Object value,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public void reconcileKeyRecordValues(String key, long record,
            Set<Object> values) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> inventory() {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecords(
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectRecordTime(long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectRecordTimestr(long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTime(
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimestr(
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Object> selectKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Set<Object> selectKeyRecordTime(String key, long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Object> selectKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectKeysRecord(List<String> keys,
            long record) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectKeysRecordTime(List<String> keys,
            long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Object>> selectKeysRecordTimestr(List<String> keys,
            long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecords(
            List<String> keys, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecords(String key,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteria(
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTime(
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimestr(
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTime(String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimestr(String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteria(String key,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTime(String key,
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimestr(String key,
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTime(String key, String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteria(
            List<String> keys, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCcl(List<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTime(
            List<String> keys, Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimestr(
            List<String> keys, Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object getKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Object getKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object getKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> getKeysRecord(List<String> keys, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> getKeysRecordTime(List<String> keys, long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> getKeysRecordTimestr(List<String> keys,
            long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecords(List<String> keys,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTime(List<String> keys,
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteria(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTime(Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimestr(Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTime(String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimestr(String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimestr(String key,
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTime(String key, String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteria(List<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCcl(List<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTime(List<String> keys,
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimestr(
            List<String> keys, Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public boolean verifyKeyValueRecord(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    public boolean verifyKeyValueRecordTime(String key, Object value,
            long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public boolean verifyKeyValueRecordTimestr(String key, Object value,
            long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public String jsonifyRecords(List<Long> records, boolean identifier) {
        throw new UnsupportedOperationException();
    }

    public String jsonifyRecordsTime(List<Long> records, long timestamp,
            boolean identifier) {
        throw new UnsupportedOperationException();
    }

    public String jsonifyRecordsTimestr(List<Long> records, String timestamp,
            boolean identifier) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCriteria(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<Object> values) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<Object> values, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimestr(String key, Operator operator,
            List<Object> values, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<Object> values) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<Object> values, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<Object> values, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> search(String key, String query) {
        throw new UnsupportedOperationException();
    }

    public void revertKeysRecordsTime(List<String> keys, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeysRecordsTimestr(List<String> keys, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeysRecordTime(List<String> keys, long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeysRecordTimestr(List<String> keys, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public void revertKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Boolean> pingRecords(List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public boolean pingRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        throw new UnsupportedOperationException();
    }

    public void verifyOrSet(String key, Object value, long record) {
        throw new UnsupportedOperationException();
    }

    public long findOrAddKeyValue(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    public long findOrInsertCriteriaJson(Criteria criteria, String json) {
        throw new UnsupportedOperationException();
    }

    public long findOrInsertCclJson(String ccl, String json) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKey(String key) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyTimestr(String key, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCriteriaTimestr(String key, Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCclTime(String key, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object sumKeyCclTimestr(String key, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecordTime(String key, long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKey(String key) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyTimestr(String key, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCriteriaTimestr(String key, Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCclTime(String key, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object averageKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKey(String key) {
        throw new UnsupportedOperationException();
    }

    public long countKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyTimestr(String key, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCriteriaTimestr(String key, Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCclTime(String key, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public long countKeyCclTimestr(String key, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCriteriaTimestr(String key, Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCclTime(String key, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyCclTimestr(String key, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKey(String key) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object maxKeyTimestr(String key, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecordTime(String key, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecordTimestr(String key, long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKey(String key) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecordsTimestr(String key, List<Long> records,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCriteriaTimestr(String key, Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCclTime(String key, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyCclTimestr(String key, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyTime(String key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyTimestr(String key, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Object minKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecordTime(String key, long record,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecordTimestr(String key,
            long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecord(
            List<String> keys, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecordTime(
            List<String> keys, long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecordTimestr(
            List<String> keys, long record, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecords(
            List<String> keys, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecords(String key,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecordsTime(String key,
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyRecordsTimestr(String key,
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCclTime(String key, String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCcl(
            List<String> keys, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCclTime(
            List<String> keys, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCclTimestr(
            List<String> keys, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCriteria(String key,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCriteriaTime(String key,
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> navigateKeyCriteriaTimestr(String key,
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCriteria(
            List<String> keys, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCriteriaTime(
            List<String> keys, Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> navigateKeysCriteriaTimestr(
            List<String> keys, Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public String getServerEnvironment() {
        throw new UnsupportedOperationException();
    }

    public String getServerVersion() {
        throw new UnsupportedOperationException();
    }

    public long time() {
        throw new UnsupportedOperationException();
    }

    public long timePhrase(String phrase) {
        throw new UnsupportedOperationException();
    }

}