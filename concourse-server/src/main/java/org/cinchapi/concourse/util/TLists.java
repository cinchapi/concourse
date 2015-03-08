/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.util;

import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Utilities for Lists.
 * 
 * @author jnelson
 */
public final class TLists {

    /**
     * Modify each list to retain only the elements that are contained in all of
     * the lists.
     * 
     * @param lists
     */
    @SafeVarargs
    public static void retainIntersection(List<?>... lists) {
        Preconditions.checkArgument(lists.length > 0);
        List<?> intersection = lists[0];
        for (int i = 1; i < lists.length; ++i) {
            intersection.retainAll(lists[i]);
        }
        for (int i = 1; i < lists.length; ++i) {
            lists[i].retainAll(intersection);
        }
    }

    private TLists() {/* noop */}

}
