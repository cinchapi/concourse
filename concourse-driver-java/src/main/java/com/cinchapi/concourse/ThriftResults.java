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
package com.cinchapi.concourse;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.LinkNavigation;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.Transformers;

/**
 * A utility class for converting thrift results to pretty java results.
 * 
 * @author Jeff Nelson
 */
final class ThriftResults {

    public static <T> Map<Long, Set<T>> prettyRecordsValues(
            Map<Long, Set<TObject>> thrift, Map<Long, Set<T>> java) {
        thrift.forEach((record, values) -> {
            java.put(record, Transformers.transformSetLazily(values,
                    Conversions.<T> thriftToJavaCasted()));
        });
        return java;
    }

    public static <T> Map<Long, Map<String, Set<T>>> prettyRecordsKeysValues(
            Map<Long, Map<String, Set<TObject>>> thrift) {
        Map<Long, Map<String, Set<T>>> java = PrettyLinkedTableMap
                .newPrettyLinkedTableMap("Record");
        return prettyRecordsKeysValues(thrift, java);
    }

    public static <T> Map<Long, Map<String, Set<T>>> prettyRecordsKeysValues(
            Map<Long, Map<String, Set<TObject>>> thrift,
            Map<Long, Map<String, Set<T>>> java) {
        thrift.forEach((record, data) -> {
            java.put(record,
                    Transformers.transformMapSet(data,
                            Conversions.<String> none(),
                            Conversions.<T> thriftToJavaCasted()));
        });
        return java;
    }

}
