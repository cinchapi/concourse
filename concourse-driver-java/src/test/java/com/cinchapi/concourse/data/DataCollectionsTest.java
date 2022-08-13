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
package com.cinchapi.concourse.data;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.data.transform.DataTable;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Unit tests for various result sets.
 *
 * @author Jeff Nelson
 */
public class DataCollectionsTest {

    @Test
    public void testRecordKeyValuesResultSet() {
        Map<Long, Map<String, Set<TObject>>> results = Maps.newLinkedHashMap();
        results.put(1L,
                ImmutableMap.of("a", ImmutableSet.of(Convert.javaToThrift(1))));
        results.put(2L, ImmutableMap.of("b", ImmutableSet
                .of(Convert.javaToThrift(11), Convert.javaToThrift(true))));
        results.put(3L,
                ImmutableMap.of("b", ImmutableSet.of(Convert.javaToThrift(100),
                        Convert.javaToThrift(false), Convert.javaToThrift("a")),
                        "c", ImmutableSet.of(Convert.javaToThrift(34))));
        Map<Long, Map<String, Set<Object>>> pretty = DataTable
                .multiValued(results);
        System.out.println(pretty);
    }

}
