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
package com.cinchapi.concourse.exporter.interactors;

import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.exporter.helpers.MapHelper;
import com.cinchapi.concourse.exporter.helpers.Tuple;

public class Exporter {
    private final Concourse concourse;
    private final boolean showPrimaryKey;

    public Exporter(Concourse concourse, boolean showPrimaryKey) {
        this.concourse = concourse;
        this.showPrimaryKey = showPrimaryKey;
    }

    public String perform() {
        return format(concourse.select(concourse.inventory()));
    }

    public String perform(Set<Long> records) {
        return format(concourse.select(records));
    }

    public String perform(String ccl) {
        return format(concourse.select(ccl));
    }

    public String perform(Set<Long> records, String ccl) {
        return format(MapHelper.filter(concourse.select(ccl),
                (k, v) -> records.contains(k)));
    }

    private String format(Map<Long, Map<String, Set<Object>>> keyedRecords) {
        final Iterable<Map<String, Set<Object>>> records = getRecords(
                keyedRecords);
    }

    private Iterable<Map<String, Set<Object>>> getRecords(
            Map<Long, Map<String, Set<Object>>> keyedRecords) {
        return MapHelper.map(keyedRecords,
                (id, xs) -> MapHelper.toMap(MapHelper.map(xs,
                        (k, v) -> new Tuple<>(
                                showPrimaryKey ? id.toString() + "," + k : k,
                                v))));
    }
}
