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
package com.cinchapi.concourse.etl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.common.collect.Collections;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

/**
 * Static methods for common data processing functions.
 *
 * @author Jeff Nelson
 */
public final class Transform {

    /**
     * Convert the {@code data} to a {@link Multimap}.
     * <p>
     * All the elements in any top-level
     * {@link com.cinchapi.common.collect.Sequences#isSequence(Object)
     * sequences} are mapped in the standard rules for {@link Multimap
     * multimaps}.
     * </p>
     * 
     * @param data
     * @return a new {@link Multimap} that contains the information in
     *         {@code data}.
     */
    public static Multimap<String, Object> toMultimap(
            Map<String, Object> data) {
        Multimap<String, Object> multimap = LinkedHashMultimap.create();
        Strainer strainer = new Strainer(
                (key, value) -> multimap.put(key, value));
        strainer.process(data);
        return multimap;
    }

    /**
     * Convert the {@code data} to a Concourse style data record where each
     * value is a collection of one or more values. No nested container values
     * are further transformed.
     * 
     * @param data
     * @return a data record
     */
    public static Map<String, Set<Object>> toRecord(Map<String, Object> data) {
        return toMultimap(data).asMap().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    return Collections.ensureSet(entry.getValue());
                }));
    }

    private Transform() {/* no-init */}

}
