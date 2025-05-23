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
package com.cinchapi.concourse.server.plugin;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
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
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TOrder objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TOrder for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> ORDER_TRANSFORM = HashMultimap
            .create();

    /**
     * A mapping from Thrift method names to a collection of parameter
     * posions that take TPage objects. For convenience, a
     * StatefulConcourseService accepts generic objects for those parameters
     * and we must keep track here so it is known what must be translated into
     * a TPage for proper routing in ConcourseServer.
     */
    protected static Multimap<String, Integer> PAGE_TRANSFORM = HashMultimap
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

        RETURN_TRANSFORM.add("selectRecordsPage");

        PAGE_TRANSFORM.put("selectRecordsPage", 1);

        RETURN_TRANSFORM.add("selectRecordsOrder");

        ORDER_TRANSFORM.put("selectRecordsOrder", 1);

        RETURN_TRANSFORM.add("selectRecordsOrderPage");

        ORDER_TRANSFORM.put("selectRecordsOrderPage", 1);

        PAGE_TRANSFORM.put("selectRecordsOrderPage", 2);

        RETURN_TRANSFORM.add("selectRecordTime");

        RETURN_TRANSFORM.add("selectRecordTimestr");

        RETURN_TRANSFORM.add("selectRecordsTime");

        RETURN_TRANSFORM.add("selectRecordsTimePage");

        PAGE_TRANSFORM.put("selectRecordsTimePage", 2);

        RETURN_TRANSFORM.add("selectRecordsTimeOrder");

        ORDER_TRANSFORM.put("selectRecordsTimeOrder", 2);

        RETURN_TRANSFORM.add("selectRecordsTimeOrderPage");

        ORDER_TRANSFORM.put("selectRecordsTimeOrderPage", 2);

        PAGE_TRANSFORM.put("selectRecordsTimeOrderPage", 3);

        RETURN_TRANSFORM.add("selectRecordsTimestr");

        RETURN_TRANSFORM.add("selectRecordsTimestrPage");

        PAGE_TRANSFORM.put("selectRecordsTimestrPage", 2);

        RETURN_TRANSFORM.add("selectRecordsTimestrOrder");

        ORDER_TRANSFORM.put("selectRecordsTimestrOrder", 2);

        RETURN_TRANSFORM.add("selectRecordsTimestrOrderPage");

        ORDER_TRANSFORM.put("selectRecordsTimestrOrderPage", 2);

        PAGE_TRANSFORM.put("selectRecordsTimestrOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyRecord");

        RETURN_TRANSFORM.add("selectKeyRecordTime");

        RETURN_TRANSFORM.add("selectKeyRecordTimestr");

        RETURN_TRANSFORM.add("selectKeysRecord");

        RETURN_TRANSFORM.add("selectKeysRecordTime");

        RETURN_TRANSFORM.add("selectKeysRecordTimestr");

        RETURN_TRANSFORM.add("selectKeysRecords");

        RETURN_TRANSFORM.add("selectKeysRecordsPage");

        PAGE_TRANSFORM.put("selectKeysRecordsPage", 2);

        RETURN_TRANSFORM.add("selectKeysRecordsOrder");

        ORDER_TRANSFORM.put("selectKeysRecordsOrder", 2);

        RETURN_TRANSFORM.add("selectKeysRecordsOrderPage");

        ORDER_TRANSFORM.put("selectKeysRecordsOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeysRecordsOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyRecords");

        RETURN_TRANSFORM.add("selectKeyRecordsPage");

        PAGE_TRANSFORM.put("selectKeyRecordsPage", 2);

        RETURN_TRANSFORM.add("selectKeyRecordsOrder");

        ORDER_TRANSFORM.put("selectKeyRecordsOrder", 2);

        RETURN_TRANSFORM.add("selectKeyRecordsOrderPage");

        ORDER_TRANSFORM.put("selectKeyRecordsOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeyRecordsOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyRecordsTime");

        RETURN_TRANSFORM.add("selectKeyRecordsTimePage");

        PAGE_TRANSFORM.put("selectKeyRecordsTimePage", 3);

        RETURN_TRANSFORM.add("selectKeyRecordsTimeOrder");

        ORDER_TRANSFORM.put("selectKeyRecordsTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeyRecordsTimeOrderPage");

        ORDER_TRANSFORM.put("selectKeyRecordsTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyRecordsTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeyRecordsTimestr");

        RETURN_TRANSFORM.add("selectKeyRecordsTimestrPage");

        PAGE_TRANSFORM.put("selectKeyRecordsTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeyRecordsTimestrOrder");

        ORDER_TRANSFORM.put("selectKeyRecordsTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeyRecordsTimestrOrderPage");

        ORDER_TRANSFORM.put("selectKeyRecordsTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyRecordsTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysRecordsTime");

        RETURN_TRANSFORM.add("selectKeysRecordsTimePage");

        PAGE_TRANSFORM.put("selectKeysRecordsTimePage", 3);

        RETURN_TRANSFORM.add("selectKeysRecordsTimeOrder");

        ORDER_TRANSFORM.put("selectKeysRecordsTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeysRecordsTimeOrderPage");

        ORDER_TRANSFORM.put("selectKeysRecordsTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysRecordsTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysRecordsTimestr");

        RETURN_TRANSFORM.add("selectKeysRecordsTimestrPage");

        PAGE_TRANSFORM.put("selectKeysRecordsTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeysRecordsTimestrOrder");

        ORDER_TRANSFORM.put("selectKeysRecordsTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeysRecordsTimestrOrderPage");

        ORDER_TRANSFORM.put("selectKeysRecordsTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysRecordsTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("selectCriteria");

        CRITERIA_TRANSFORM.put("selectCriteria", 0);

        RETURN_TRANSFORM.add("selectCriteriaPage");

        CRITERIA_TRANSFORM.put("selectCriteriaPage", 0);

        PAGE_TRANSFORM.put("selectCriteriaPage", 1);

        RETURN_TRANSFORM.add("selectCriteriaOrder");

        CRITERIA_TRANSFORM.put("selectCriteriaOrder", 0);

        ORDER_TRANSFORM.put("selectCriteriaOrder", 1);

        RETURN_TRANSFORM.add("selectCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("selectCriteriaOrderPage", 0);

        ORDER_TRANSFORM.put("selectCriteriaOrderPage", 1);

        PAGE_TRANSFORM.put("selectCriteriaOrderPage", 2);

        RETURN_TRANSFORM.add("selectCcl");

        RETURN_TRANSFORM.add("selectCclPage");

        PAGE_TRANSFORM.put("selectCclPage", 1);

        RETURN_TRANSFORM.add("selectCclOrder");

        ORDER_TRANSFORM.put("selectCclOrder", 1);

        RETURN_TRANSFORM.add("selectCclOrderPage");

        ORDER_TRANSFORM.put("selectCclOrderPage", 1);

        PAGE_TRANSFORM.put("selectCclOrderPage", 2);

        RETURN_TRANSFORM.add("selectCriteriaTime");

        CRITERIA_TRANSFORM.put("selectCriteriaTime", 0);

        RETURN_TRANSFORM.add("selectCriteriaTimePage");

        CRITERIA_TRANSFORM.put("selectCriteriaTimePage", 0);

        PAGE_TRANSFORM.put("selectCriteriaTimePage", 2);

        RETURN_TRANSFORM.add("selectCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("selectCriteriaTimeOrder", 0);

        ORDER_TRANSFORM.put("selectCriteriaTimeOrder", 2);

        RETURN_TRANSFORM.add("selectCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("selectCriteriaTimeOrderPage", 0);

        ORDER_TRANSFORM.put("selectCriteriaTimeOrderPage", 2);

        PAGE_TRANSFORM.put("selectCriteriaTimeOrderPage", 3);

        RETURN_TRANSFORM.add("selectCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectCriteriaTimestr", 0);

        RETURN_TRANSFORM.add("selectCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("selectCriteriaTimestrPage", 0);

        PAGE_TRANSFORM.put("selectCriteriaTimestrPage", 2);

        RETURN_TRANSFORM.add("selectCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("selectCriteriaTimestrOrder", 0);

        ORDER_TRANSFORM.put("selectCriteriaTimestrOrder", 2);

        RETURN_TRANSFORM.add("selectCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("selectCriteriaTimestrOrderPage", 0);

        ORDER_TRANSFORM.put("selectCriteriaTimestrOrderPage", 2);

        PAGE_TRANSFORM.put("selectCriteriaTimestrOrderPage", 3);

        RETURN_TRANSFORM.add("selectCclTime");

        RETURN_TRANSFORM.add("selectCclTimePage");

        PAGE_TRANSFORM.put("selectCclTimePage", 2);

        RETURN_TRANSFORM.add("selectCclTimeOrder");

        ORDER_TRANSFORM.put("selectCclTimeOrder", 2);

        RETURN_TRANSFORM.add("selectCclTimeOrderPage");

        ORDER_TRANSFORM.put("selectCclTimeOrderPage", 2);

        PAGE_TRANSFORM.put("selectCclTimeOrderPage", 3);

        RETURN_TRANSFORM.add("selectCclTimestr");

        RETURN_TRANSFORM.add("selectCclTimestrPage");

        PAGE_TRANSFORM.put("selectCclTimestrPage", 2);

        RETURN_TRANSFORM.add("selectCclTimestrOrder");

        ORDER_TRANSFORM.put("selectCclTimestrOrder", 2);

        RETURN_TRANSFORM.add("selectCclTimestrOrderPage");

        ORDER_TRANSFORM.put("selectCclTimestrOrderPage", 2);

        PAGE_TRANSFORM.put("selectCclTimestrOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyCriteria");

        CRITERIA_TRANSFORM.put("selectKeyCriteria", 1);

        RETURN_TRANSFORM.add("selectKeyCriteriaPage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaPage", 1);

        PAGE_TRANSFORM.put("selectKeyCriteriaPage", 2);

        RETURN_TRANSFORM.add("selectKeyCriteriaOrder");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaOrder", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaOrder", 2);

        RETURN_TRANSFORM.add("selectKeyCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeyCriteriaOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyCcl");

        RETURN_TRANSFORM.add("selectKeyCclPage");

        PAGE_TRANSFORM.put("selectKeyCclPage", 2);

        RETURN_TRANSFORM.add("selectKeyCclOrder");

        ORDER_TRANSFORM.put("selectKeyCclOrder", 2);

        RETURN_TRANSFORM.add("selectKeyCclOrderPage");

        ORDER_TRANSFORM.put("selectKeyCclOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeyCclOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimePage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimePage", 1);

        PAGE_TRANSFORM.put("selectKeyCriteriaTimePage", 3);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimeOrder", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimeOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyCriteriaTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimestrPage", 1);

        PAGE_TRANSFORM.put("selectKeyCriteriaTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimestrOrder", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeyCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("selectKeyCriteriaTimestrOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeyCriteriaTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyCriteriaTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeyCclTime");

        RETURN_TRANSFORM.add("selectKeyCclTimePage");

        PAGE_TRANSFORM.put("selectKeyCclTimePage", 3);

        RETURN_TRANSFORM.add("selectKeyCclTimeOrder");

        ORDER_TRANSFORM.put("selectKeyCclTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeyCclTimeOrderPage");

        ORDER_TRANSFORM.put("selectKeyCclTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyCclTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeyCclTimestr");

        RETURN_TRANSFORM.add("selectKeyCclTimestrPage");

        PAGE_TRANSFORM.put("selectKeyCclTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeyCclTimestrOrder");

        ORDER_TRANSFORM.put("selectKeyCclTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeyCclTimestrOrderPage");

        ORDER_TRANSFORM.put("selectKeyCclTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeyCclTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysCriteria");

        CRITERIA_TRANSFORM.put("selectKeysCriteria", 1);

        RETURN_TRANSFORM.add("selectKeysCriteriaPage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaPage", 1);

        PAGE_TRANSFORM.put("selectKeysCriteriaPage", 2);

        RETURN_TRANSFORM.add("selectKeysCriteriaOrder");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaOrder", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaOrder", 2);

        RETURN_TRANSFORM.add("selectKeysCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeysCriteriaOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeysCcl");

        RETURN_TRANSFORM.add("selectKeysCclPage");

        PAGE_TRANSFORM.put("selectKeysCclPage", 2);

        RETURN_TRANSFORM.add("selectKeysCclOrder");

        ORDER_TRANSFORM.put("selectKeysCclOrder", 2);

        RETURN_TRANSFORM.add("selectKeysCclOrderPage");

        ORDER_TRANSFORM.put("selectKeysCclOrderPage", 2);

        PAGE_TRANSFORM.put("selectKeysCclOrderPage", 3);

        RETURN_TRANSFORM.add("selectKeysCriteriaTime");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTime", 1);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimePage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimePage", 1);

        PAGE_TRANSFORM.put("selectKeysCriteriaTimePage", 3);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimeOrder", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimeOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysCriteriaTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimestr");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimestrPage", 1);

        PAGE_TRANSFORM.put("selectKeysCriteriaTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimestrOrder", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeysCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("selectKeysCriteriaTimestrOrderPage", 1);

        ORDER_TRANSFORM.put("selectKeysCriteriaTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysCriteriaTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysCclTime");

        RETURN_TRANSFORM.add("selectKeysCclTimePage");

        PAGE_TRANSFORM.put("selectKeysCclTimePage", 3);

        RETURN_TRANSFORM.add("selectKeysCclTimeOrder");

        ORDER_TRANSFORM.put("selectKeysCclTimeOrder", 3);

        RETURN_TRANSFORM.add("selectKeysCclTimeOrderPage");

        ORDER_TRANSFORM.put("selectKeysCclTimeOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysCclTimeOrderPage", 4);

        RETURN_TRANSFORM.add("selectKeysCclTimestr");

        RETURN_TRANSFORM.add("selectKeysCclTimestrPage");

        PAGE_TRANSFORM.put("selectKeysCclTimestrPage", 3);

        RETURN_TRANSFORM.add("selectKeysCclTimestrOrder");

        ORDER_TRANSFORM.put("selectKeysCclTimestrOrder", 3);

        RETURN_TRANSFORM.add("selectKeysCclTimestrOrderPage");

        ORDER_TRANSFORM.put("selectKeysCclTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("selectKeysCclTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyRecord");

        RETURN_TRANSFORM.add("getKeyRecordTime");

        RETURN_TRANSFORM.add("getKeyRecordTimestr");

        RETURN_TRANSFORM.add("getKeysRecord");

        RETURN_TRANSFORM.add("getKeysRecordTime");

        RETURN_TRANSFORM.add("getKeysRecordTimestr");

        RETURN_TRANSFORM.add("getKeysRecords");

        RETURN_TRANSFORM.add("getKeysRecordsPage");

        PAGE_TRANSFORM.put("getKeysRecordsPage", 2);

        RETURN_TRANSFORM.add("getKeysRecordsOrder");

        ORDER_TRANSFORM.put("getKeysRecordsOrder", 2);

        RETURN_TRANSFORM.add("getKeysRecordsOrderPage");

        ORDER_TRANSFORM.put("getKeysRecordsOrderPage", 2);

        PAGE_TRANSFORM.put("getKeysRecordsOrderPage", 3);

        RETURN_TRANSFORM.add("getKeyRecords");

        RETURN_TRANSFORM.add("getKeyRecordsPage");

        PAGE_TRANSFORM.put("getKeyRecordsPage", 2);

        RETURN_TRANSFORM.add("getKeyRecordsOrder");

        ORDER_TRANSFORM.put("getKeyRecordsOrder", 2);

        RETURN_TRANSFORM.add("getKeyRecordsOrderPage");

        ORDER_TRANSFORM.put("getKeyRecordsOrderPage", 2);

        PAGE_TRANSFORM.put("getKeyRecordsOrderPage", 3);

        RETURN_TRANSFORM.add("getKeyRecordsTime");

        RETURN_TRANSFORM.add("getKeyRecordsTimePage");

        PAGE_TRANSFORM.put("getKeyRecordsTimePage", 3);

        RETURN_TRANSFORM.add("getKeyRecordsTimeOrder");

        ORDER_TRANSFORM.put("getKeyRecordsTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeyRecordsTimeOrderPage");

        ORDER_TRANSFORM.put("getKeyRecordsTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyRecordsTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyRecordsTimestr");

        RETURN_TRANSFORM.add("getKeyRecordsTimestrPage");

        PAGE_TRANSFORM.put("getKeyRecordsTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeyRecordsTimestrOrder");

        ORDER_TRANSFORM.put("getKeyRecordsTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeyRecordsTimestrOrderPage");

        ORDER_TRANSFORM.put("getKeyRecordsTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyRecordsTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysRecordsTime");

        RETURN_TRANSFORM.add("getKeysRecordsTimePage");

        PAGE_TRANSFORM.put("getKeysRecordsTimePage", 3);

        RETURN_TRANSFORM.add("getKeysRecordsTimeOrder");

        ORDER_TRANSFORM.put("getKeysRecordsTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeysRecordsTimeOrderPage");

        ORDER_TRANSFORM.put("getKeysRecordsTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysRecordsTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysRecordsTimestr");

        RETURN_TRANSFORM.add("getKeysRecordsTimestrPage");

        PAGE_TRANSFORM.put("getKeysRecordsTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeysRecordsTimestrOrder");

        ORDER_TRANSFORM.put("getKeysRecordsTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeysRecordsTimestrOrderPage");

        ORDER_TRANSFORM.put("getKeysRecordsTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysRecordsTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyCriteria");

        CRITERIA_TRANSFORM.put("getKeyCriteria", 1);

        RETURN_TRANSFORM.add("getKeyCriteriaPage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaPage", 1);

        PAGE_TRANSFORM.put("getKeyCriteriaPage", 2);

        RETURN_TRANSFORM.add("getKeyCriteriaOrder");

        CRITERIA_TRANSFORM.put("getKeyCriteriaOrder", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaOrder", 2);

        RETURN_TRANSFORM.add("getKeyCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaOrderPage", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaOrderPage", 2);

        PAGE_TRANSFORM.put("getKeyCriteriaOrderPage", 3);

        RETURN_TRANSFORM.add("getCriteria");

        CRITERIA_TRANSFORM.put("getCriteria", 0);

        RETURN_TRANSFORM.add("getCriteriaPage");

        CRITERIA_TRANSFORM.put("getCriteriaPage", 0);

        PAGE_TRANSFORM.put("getCriteriaPage", 1);

        RETURN_TRANSFORM.add("getCriteriaOrder");

        CRITERIA_TRANSFORM.put("getCriteriaOrder", 0);

        ORDER_TRANSFORM.put("getCriteriaOrder", 1);

        RETURN_TRANSFORM.add("getCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("getCriteriaOrderPage", 0);

        ORDER_TRANSFORM.put("getCriteriaOrderPage", 1);

        PAGE_TRANSFORM.put("getCriteriaOrderPage", 2);

        RETURN_TRANSFORM.add("getCcl");

        RETURN_TRANSFORM.add("getCclPage");

        PAGE_TRANSFORM.put("getCclPage", 1);

        RETURN_TRANSFORM.add("getCclOrder");

        ORDER_TRANSFORM.put("getCclOrder", 1);

        RETURN_TRANSFORM.add("getCclOrderPage");

        ORDER_TRANSFORM.put("getCclOrderPage", 1);

        PAGE_TRANSFORM.put("getCclOrderPage", 2);

        RETURN_TRANSFORM.add("getCriteriaTime");

        CRITERIA_TRANSFORM.put("getCriteriaTime", 0);

        RETURN_TRANSFORM.add("getCriteriaTimePage");

        CRITERIA_TRANSFORM.put("getCriteriaTimePage", 0);

        PAGE_TRANSFORM.put("getCriteriaTimePage", 2);

        RETURN_TRANSFORM.add("getCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("getCriteriaTimeOrder", 0);

        ORDER_TRANSFORM.put("getCriteriaTimeOrder", 2);

        RETURN_TRANSFORM.add("getCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("getCriteriaTimeOrderPage", 0);

        ORDER_TRANSFORM.put("getCriteriaTimeOrderPage", 2);

        PAGE_TRANSFORM.put("getCriteriaTimeOrderPage", 3);

        RETURN_TRANSFORM.add("getCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getCriteriaTimestr", 0);

        RETURN_TRANSFORM.add("getCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("getCriteriaTimestrPage", 0);

        PAGE_TRANSFORM.put("getCriteriaTimestrPage", 2);

        RETURN_TRANSFORM.add("getCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("getCriteriaTimestrOrder", 0);

        ORDER_TRANSFORM.put("getCriteriaTimestrOrder", 2);

        RETURN_TRANSFORM.add("getCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("getCriteriaTimestrOrderPage", 0);

        ORDER_TRANSFORM.put("getCriteriaTimestrOrderPage", 2);

        PAGE_TRANSFORM.put("getCriteriaTimestrOrderPage", 3);

        RETURN_TRANSFORM.add("getCclTime");

        RETURN_TRANSFORM.add("getCclTimePage");

        PAGE_TRANSFORM.put("getCclTimePage", 2);

        RETURN_TRANSFORM.add("getCclTimeOrder");

        ORDER_TRANSFORM.put("getCclTimeOrder", 2);

        RETURN_TRANSFORM.add("getCclTimeOrderPage");

        ORDER_TRANSFORM.put("getCclTimeOrderPage", 2);

        PAGE_TRANSFORM.put("getCclTimeOrderPage", 3);

        RETURN_TRANSFORM.add("getCclTimestr");

        RETURN_TRANSFORM.add("getCclTimestrPage");

        PAGE_TRANSFORM.put("getCclTimestrPage", 2);

        RETURN_TRANSFORM.add("getCclTimestrOrder");

        ORDER_TRANSFORM.put("getCclTimestrOrder", 2);

        RETURN_TRANSFORM.add("getCclTimestrOrderPage");

        ORDER_TRANSFORM.put("getCclTimestrOrderPage", 2);

        PAGE_TRANSFORM.put("getCclTimestrOrderPage", 3);

        RETURN_TRANSFORM.add("getKeyCcl");

        RETURN_TRANSFORM.add("getKeyCclPage");

        PAGE_TRANSFORM.put("getKeyCclPage", 2);

        RETURN_TRANSFORM.add("getKeyCclOrder");

        ORDER_TRANSFORM.put("getKeyCclOrder", 2);

        RETURN_TRANSFORM.add("getKeyCclOrderPage");

        ORDER_TRANSFORM.put("getKeyCclOrderPage", 2);

        PAGE_TRANSFORM.put("getKeyCclOrderPage", 3);

        RETURN_TRANSFORM.add("getKeyCriteriaTime");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTime", 1);

        RETURN_TRANSFORM.add("getKeyCriteriaTimePage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimePage", 1);

        PAGE_TRANSFORM.put("getKeyCriteriaTimePage", 3);

        RETURN_TRANSFORM.add("getKeyCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimeOrder", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeyCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimeOrderPage", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyCriteriaTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("getKeyCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimestrPage", 1);

        PAGE_TRANSFORM.put("getKeyCriteriaTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeyCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimestrOrder", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeyCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("getKeyCriteriaTimestrOrderPage", 1);

        ORDER_TRANSFORM.put("getKeyCriteriaTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyCriteriaTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyCclTime");

        RETURN_TRANSFORM.add("getKeyCclTimePage");

        PAGE_TRANSFORM.put("getKeyCclTimePage", 3);

        RETURN_TRANSFORM.add("getKeyCclTimeOrder");

        ORDER_TRANSFORM.put("getKeyCclTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeyCclTimeOrderPage");

        ORDER_TRANSFORM.put("getKeyCclTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyCclTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeyCclTimestr");

        RETURN_TRANSFORM.add("getKeyCclTimestrPage");

        PAGE_TRANSFORM.put("getKeyCclTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeyCclTimestrOrder");

        ORDER_TRANSFORM.put("getKeyCclTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeyCclTimestrOrderPage");

        ORDER_TRANSFORM.put("getKeyCclTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeyCclTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysCriteria");

        CRITERIA_TRANSFORM.put("getKeysCriteria", 1);

        RETURN_TRANSFORM.add("getKeysCriteriaPage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaPage", 1);

        PAGE_TRANSFORM.put("getKeysCriteriaPage", 2);

        RETURN_TRANSFORM.add("getKeysCriteriaOrder");

        CRITERIA_TRANSFORM.put("getKeysCriteriaOrder", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaOrder", 2);

        RETURN_TRANSFORM.add("getKeysCriteriaOrderPage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaOrderPage", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaOrderPage", 2);

        PAGE_TRANSFORM.put("getKeysCriteriaOrderPage", 3);

        RETURN_TRANSFORM.add("getKeysCcl");

        RETURN_TRANSFORM.add("getKeysCclPage");

        PAGE_TRANSFORM.put("getKeysCclPage", 2);

        RETURN_TRANSFORM.add("getKeysCclOrder");

        ORDER_TRANSFORM.put("getKeysCclOrder", 2);

        RETURN_TRANSFORM.add("getKeysCclOrderPage");

        ORDER_TRANSFORM.put("getKeysCclOrderPage", 2);

        PAGE_TRANSFORM.put("getKeysCclOrderPage", 3);

        RETURN_TRANSFORM.add("getKeysCriteriaTime");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTime", 1);

        RETURN_TRANSFORM.add("getKeysCriteriaTimePage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimePage", 1);

        PAGE_TRANSFORM.put("getKeysCriteriaTimePage", 3);

        RETURN_TRANSFORM.add("getKeysCriteriaTimeOrder");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimeOrder", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeysCriteriaTimeOrderPage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimeOrderPage", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysCriteriaTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysCriteriaTimestr");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimestr", 1);

        RETURN_TRANSFORM.add("getKeysCriteriaTimestrPage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimestrPage", 1);

        PAGE_TRANSFORM.put("getKeysCriteriaTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeysCriteriaTimestrOrder");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimestrOrder", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeysCriteriaTimestrOrderPage");

        CRITERIA_TRANSFORM.put("getKeysCriteriaTimestrOrderPage", 1);

        ORDER_TRANSFORM.put("getKeysCriteriaTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysCriteriaTimestrOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysCclTime");

        RETURN_TRANSFORM.add("getKeysCclTimePage");

        PAGE_TRANSFORM.put("getKeysCclTimePage", 3);

        RETURN_TRANSFORM.add("getKeysCclTimeOrder");

        ORDER_TRANSFORM.put("getKeysCclTimeOrder", 3);

        RETURN_TRANSFORM.add("getKeysCclTimeOrderPage");

        ORDER_TRANSFORM.put("getKeysCclTimeOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysCclTimeOrderPage", 4);

        RETURN_TRANSFORM.add("getKeysCclTimestr");

        RETURN_TRANSFORM.add("getKeysCclTimestrPage");

        PAGE_TRANSFORM.put("getKeysCclTimestrPage", 3);

        RETURN_TRANSFORM.add("getKeysCclTimestrOrder");

        ORDER_TRANSFORM.put("getKeysCclTimestrOrder", 3);

        RETURN_TRANSFORM.add("getKeysCclTimestrOrderPage");

        ORDER_TRANSFORM.put("getKeysCclTimestrOrderPage", 3);

        PAGE_TRANSFORM.put("getKeysCclTimestrOrderPage", 4);

        VALUE_TRANSFORM.put("verifyKeyValueRecord", 1);

        VALUE_TRANSFORM.put("verifyKeyValueRecordTime", 1);

        VALUE_TRANSFORM.put("verifyKeyValueRecordTimestr", 1);

        CRITERIA_TRANSFORM.put("findCriteria", 0);

        CRITERIA_TRANSFORM.put("findCriteriaPage", 0);

        PAGE_TRANSFORM.put("findCriteriaPage", 1);

        CRITERIA_TRANSFORM.put("findCriteriaOrder", 0);

        ORDER_TRANSFORM.put("findCriteriaOrder", 1);

        CRITERIA_TRANSFORM.put("findCriteriaOrderPage", 0);

        ORDER_TRANSFORM.put("findCriteriaOrderPage", 1);

        PAGE_TRANSFORM.put("findCriteriaOrderPage", 2);

        PAGE_TRANSFORM.put("findCclPage", 1);

        ORDER_TRANSFORM.put("findCclOrder", 1);

        ORDER_TRANSFORM.put("findCclOrderPage", 1);

        PAGE_TRANSFORM.put("findCclOrderPage", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValues", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValuesPage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorValuesPage", 3);

        VALUE_TRANSFORM.put("findKeyOperatorValuesOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesOrder", 3);

        VALUE_TRANSFORM.put("findKeyOperatorValuesOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesOrderPage", 3);

        PAGE_TRANSFORM.put("findKeyOperatorValuesOrderPage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTime", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimePage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorValuesTimePage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimeOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesTimeOrder", 4);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimeOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesTimeOrderPage", 4);

        PAGE_TRANSFORM.put("findKeyOperatorValuesTimeOrderPage", 5);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimestr", 2);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimestrPage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorValuesTimestrPage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimestrOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesTimestrOrder", 4);

        VALUE_TRANSFORM.put("findKeyOperatorValuesTimestrOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorValuesTimestrOrderPage", 4);

        PAGE_TRANSFORM.put("findKeyOperatorValuesTimestrOrderPage", 5);

        VALUE_TRANSFORM.put("findKeyOperatorstrValues", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesPage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesPage", 3);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesOrder", 3);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesOrderPage", 3);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesOrderPage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTime", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimePage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesTimePage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimeOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesTimeOrder", 4);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimeOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesTimeOrderPage", 4);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesTimeOrderPage", 5);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimestr", 2);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimestrPage", 2);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesTimestrPage", 4);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimestrOrder", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesTimestrOrder", 4);

        VALUE_TRANSFORM.put("findKeyOperatorstrValuesTimestrOrderPage", 2);

        ORDER_TRANSFORM.put("findKeyOperatorstrValuesTimestrOrderPage", 4);

        PAGE_TRANSFORM.put("findKeyOperatorstrValuesTimestrOrderPage", 5);

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

    public Map<Long, List<String>> reviewRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewRecordStart(long record, long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewRecordStartstr(long record,
            String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewRecordStartEnd(long record, long start,
            long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewRecordStartstrEndstr(long record,
            String start, String tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewKeyRecord(String key, long record) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewKeyRecordStart(String key, long record,
            long start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewKeyRecordStartstr(String key,
            long record, String start) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewKeyRecordStartEnd(String key,
            long record, long start, long tend) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, List<String>> reviewKeyRecordStartstrEndstr(String key,
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

    public Map<Long, Map<String, Set<Object>>> selectRecordsPage(
            List<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsOrder(
            List<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsOrderPage(
            List<Long> records, Order order, Page page) {
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

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimePage(
            List<Long> records, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimeOrder(
            List<Long> records, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimeOrderPage(
            List<Long> records, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimestr(
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimestrPage(
            List<Long> records, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimestrOrder(
            List<Long> records, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectRecordsTimestrOrderPage(
            List<Long> records, String timestamp, Order order, Page page) {
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

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsPage(
            List<String> keys, List<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsOrder(
            List<String> keys, List<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsOrderPage(
            List<String> keys, List<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecords(String key,
            List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsPage(String key,
            List<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsOrder(String key,
            List<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsOrderPage(String key,
            List<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTime(String key,
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimePage(String key,
            List<Long> records, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimeOrder(String key,
            List<Long> records, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimeOrderPage(String key,
            List<Long> records, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimestr(String key,
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimestrPage(String key,
            List<Long> records, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimestrOrder(String key,
            List<Long> records, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyRecordsTimestrOrderPage(String key,
            List<Long> records, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTime(
            List<String> keys, List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimePage(
            List<String> keys, List<Long> records, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimeOrder(
            List<String> keys, List<Long> records, long timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimeOrderPage(
            List<String> keys, List<Long> records, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimestrPage(
            List<String> keys, List<Long> records, String timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimestrOrder(
            List<String> keys, List<Long> records, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysRecordsTimestrOrderPage(
            List<String> keys, List<Long> records, String timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteria(
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaPage(
            Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaOrder(
            Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaOrderPage(
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclPage(String ccl,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclOrder(String ccl,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclOrderPage(String ccl,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTime(
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimePage(
            Criteria criteria, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimeOrder(
            Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimeOrderPage(
            Criteria criteria, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimestr(
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimestrPage(
            Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimestrOrder(
            Criteria criteria, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCriteriaTimestrOrderPage(
            Criteria criteria, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTime(String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimePage(String ccl,
            long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimeOrder(String ccl,
            long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimeOrderPage(
            String ccl, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimestr(String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimestrPage(String ccl,
            String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimestrOrder(String ccl,
            String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectCclTimestrOrderPage(
            String ccl, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteria(String key,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaPage(String key,
            Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaOrder(String key,
            Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaOrderPage(String key,
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclPage(String key, String ccl,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclOrder(String key, String ccl,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclOrderPage(String key, String ccl,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTime(String key,
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimePage(String key,
            Criteria criteria, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimeOrder(String key,
            Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimeOrderPage(String key,
            Criteria criteria, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimestr(String key,
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimestrPage(String key,
            Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimestrOrder(String key,
            Criteria criteria, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCriteriaTimestrOrderPage(String key,
            Criteria criteria, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTime(String key, String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimePage(String key, String ccl,
            long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimeOrder(String key, String ccl,
            long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimeOrderPage(String key,
            String ccl, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimestrPage(String key,
            String ccl, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimestrOrder(String key,
            String ccl, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Set<Object>> selectKeyCclTimestrOrderPage(String key,
            String ccl, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteria(
            List<String> keys, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaPage(
            List<String> keys, Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaOrder(
            List<String> keys, Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaOrderPage(
            List<String> keys, Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCcl(List<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclPage(
            List<String> keys, String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclOrder(
            List<String> keys, String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclOrderPage(
            List<String> keys, String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTime(
            List<String> keys, Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimePage(
            List<String> keys, Criteria criteria, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimeOrder(
            List<String> keys, Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimeOrderPage(
            List<String> keys, Criteria criteria, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimestr(
            List<String> keys, Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimestrPage(
            List<String> keys, Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimestrOrder(
            List<String> keys, Criteria criteria, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCriteriaTimestrOrderPage(
            List<String> keys, Criteria criteria, String timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTime(
            List<String> keys, String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimePage(
            List<String> keys, String ccl, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimeOrder(
            List<String> keys, String ccl, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimeOrderPage(
            List<String> keys, String ccl, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimestr(
            List<String> keys, String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimestrPage(
            List<String> keys, String ccl, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimestrOrder(
            List<String> keys, String ccl, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Object>>> selectKeysCclTimestrOrderPage(
            List<String> keys, String ccl, String timestamp, Order order,
            Page page) {
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

    public Map<Long, Map<String, Object>> getKeysRecordsPage(List<String> keys,
            List<Long> records, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsOrder(List<String> keys,
            List<Long> records, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsOrderPage(
            List<String> keys, List<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecords(String key, List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsPage(String key, List<Long> records,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsOrder(String key, List<Long> records,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsOrderPage(String key,
            List<Long> records, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTime(String key, List<Long> records,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimePage(String key,
            List<Long> records, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimeOrder(String key,
            List<Long> records, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimeOrderPage(String key,
            List<Long> records, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimestr(String key,
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimestrPage(String key,
            List<Long> records, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimestrOrder(String key,
            List<Long> records, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyRecordsTimestrOrderPage(String key,
            List<Long> records, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTime(List<String> keys,
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimePage(
            List<String> keys, List<Long> records, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimeOrder(
            List<String> keys, List<Long> records, long timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimeOrderPage(
            List<String> keys, List<Long> records, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimestr(
            List<String> keys, List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimestrPage(
            List<String> keys, List<Long> records, String timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimestrOrder(
            List<String> keys, List<Long> records, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysRecordsTimestrOrderPage(
            List<String> keys, List<Long> records, String timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteria(String key, Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaPage(String key, Criteria criteria,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaOrder(String key, Criteria criteria,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaOrderPage(String key,
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteria(Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaPage(Criteria criteria,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaOrder(Criteria criteria,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaOrderPage(
            Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclPage(String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclOrder(String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclOrderPage(String ccl,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTime(Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimePage(Criteria criteria,
            long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimeOrder(
            Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimeOrderPage(
            Criteria criteria, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimestr(Criteria criteria,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimestrPage(
            Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimestrOrder(
            Criteria criteria, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCriteriaTimestrOrderPage(
            Criteria criteria, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTime(String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimePage(String ccl,
            long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimeOrder(String ccl,
            long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimeOrderPage(String ccl,
            long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimestr(String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimestrPage(String ccl,
            String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimestrOrder(String ccl,
            String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getCclTimestrOrderPage(String ccl,
            String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCcl(String key, String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclPage(String key, String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclOrder(String key, String ccl,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclOrderPage(String key, String ccl,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTime(String key, Criteria criteria,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimePage(String key,
            Criteria criteria, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimeOrder(String key,
            Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimeOrderPage(String key,
            Criteria criteria, long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimestr(String key,
            Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimestrPage(String key,
            Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimestrOrder(String key,
            Criteria criteria, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCriteriaTimestrOrderPage(String key,
            Criteria criteria, String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTime(String key, String ccl,
            long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimePage(String key, String ccl,
            long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimeOrder(String key, String ccl,
            long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimeOrderPage(String key, String ccl,
            long timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimestr(String key, String ccl,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimestrPage(String key, String ccl,
            String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimestrOrder(String key, String ccl,
            String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Object> getKeyCclTimestrOrderPage(String key, String ccl,
            String timestamp, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteria(List<String> keys,
            Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaPage(List<String> keys,
            Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaOrder(
            List<String> keys, Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaOrderPage(
            List<String> keys, Criteria criteria, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCcl(List<String> keys,
            String ccl) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclPage(List<String> keys,
            String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclOrder(List<String> keys,
            String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclOrderPage(List<String> keys,
            String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTime(List<String> keys,
            Criteria criteria, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimePage(
            List<String> keys, Criteria criteria, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimeOrder(
            List<String> keys, Criteria criteria, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimeOrderPage(
            List<String> keys, Criteria criteria, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimestr(
            List<String> keys, Criteria criteria, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimestrPage(
            List<String> keys, Criteria criteria, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimestrOrder(
            List<String> keys, Criteria criteria, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCriteriaTimestrOrderPage(
            List<String> keys, Criteria criteria, String timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTime(List<String> keys,
            String ccl, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimePage(List<String> keys,
            String ccl, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimeOrder(List<String> keys,
            String ccl, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimeOrderPage(
            List<String> keys, String ccl, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimestr(List<String> keys,
            String ccl, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimestrPage(
            List<String> keys, String ccl, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimestrOrder(
            List<String> keys, String ccl, String timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Object>> getKeysCclTimestrOrderPage(
            List<String> keys, String ccl, String timestamp, Order order,
            Page page) {
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

    public Set<Long> findCriteriaPage(Criteria criteria, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCriteriaOrder(Criteria criteria, Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCriteriaOrderPage(Criteria criteria, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCcl(String ccl) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCclPage(String ccl, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCclOrder(String ccl, Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findCclOrderPage(String ccl, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValues(String key, Operator operator,
            List<Object> values) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesPage(String key, Operator operator,
            List<Object> values, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesOrder(String key, Operator operator,
            List<Object> values, Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesOrderPage(String key,
            Operator operator, List<Object> values, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTime(String key, Operator operator,
            List<Object> values, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimePage(String key,
            Operator operator, List<Object> values, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimeOrder(String key,
            Operator operator, List<Object> values, long timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimeOrderPage(String key,
            Operator operator, List<Object> values, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimestr(String key, Operator operator,
            List<Object> values, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimestrPage(String key,
            Operator operator, List<Object> values, String timestamp,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimestrOrder(String key,
            Operator operator, List<Object> values, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorValuesTimestrOrderPage(String key,
            Operator operator, List<Object> values, String timestamp,
            Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValues(String key, String operator,
            List<Object> values) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesPage(String key, String operator,
            List<Object> values, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesOrder(String key, String operator,
            List<Object> values, Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesOrderPage(String key,
            String operator, List<Object> values, Order order, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTime(String key, String operator,
            List<Object> values, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimePage(String key,
            String operator, List<Object> values, long timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimeOrder(String key,
            String operator, List<Object> values, long timestamp, Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimeOrderPage(String key,
            String operator, List<Object> values, long timestamp, Order order,
            Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimestr(String key,
            String operator, List<Object> values, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimestrPage(String key,
            String operator, List<Object> values, String timestamp, Page page) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimestrOrder(String key,
            String operator, List<Object> values, String timestamp,
            Order order) {
        throw new UnsupportedOperationException();
    }

    public Set<Long> findKeyOperatorstrValuesTimestrOrderPage(String key,
            String operator, List<Object> values, String timestamp, Order order,
            Page page) {
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

    public Map<String, Set<Long>> traceRecord(long record) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Long>> traceRecordTime(long record, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Set<Long>> traceRecordTimestr(long record,
            String timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Long>>> traceRecords(List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Long>>> traceRecordsTime(
            List<Long> records, long timestamp) {
        throw new UnsupportedOperationException();
    }

    public Map<Long, Map<String, Set<Long>>> traceRecordsTimestr(
            List<Long> records, String timestamp) {
        throw new UnsupportedOperationException();
    }

    public boolean consolidateRecords(List<Long> records) {
        throw new UnsupportedOperationException();
    }

    public boolean ping() {
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

}