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
package com.cinchapi.concourse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.cinchapi.concourse.util.PrettyLinkedHashMap;

/**
 * Contains functions for preserving backwards compatibility.
 *
 * @author Jeff Nelson
 */
public final class BackwardsCompatability {

    /**
     * Transform a value returned from {@link Concourse#review(long) review}
     * methods to one that should be returned from {@link Concourse#audit(long)
     * audit} methods.
     * 
     * @param review
     * @return the transformed value
     */
    public static <K> Map<K, String> auditFromReview(
            Map<K, List<String>> review) {
        return review.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey,
                        entry -> entry.getValue().size() == 1
                                ? entry.getValue().iterator().next().toString()
                                : entry.getValue().toString(),
                        (a, b) -> a, PrettyLinkedHashMap::create));
    }

    private BackwardsCompatability() {/* no-init */}

}
