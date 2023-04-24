/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.sort;

import java.util.Comparator;

/**
 * Utilities for {@link Comparator}s.
 *
 * @author Jeff Nelson
 */
public class Comparators {

    /**
     * Return a version of the {@code comparator} that can handle null values.
     * 
     * @param comparator
     * @return a null-safe version of {@code comparator}
     */
    public static <T> Comparator<T> nullSafe(Comparator<T> comparator) {
        return (o1, o2) -> {
            if(o1 != null && o2 != null) {
                return comparator.compare(o1, o2);
            }
            else if(o1 == null) {
                return 1;
            }
            else if(o2 == null) {
                return -1;
            }
            else {
                return 0;
            }
        };
    }

}
