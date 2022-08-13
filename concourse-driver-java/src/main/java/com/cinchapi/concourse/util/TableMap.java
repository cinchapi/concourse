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
package com.cinchapi.concourse.util;

import java.util.Map;

/**
 * Two-dimensional mapping that associates a "row" key to another
 * {@link Map} where "column" keys are associated with a "value".
 * <p>
 * Conceptually, similar to {@link com.google.common.collect.Table}, but this
 * implementation is not as full featured.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface TableMap<R, C, V> extends Map<R, Map<C, V>> {

    /**
     * Insert {@code value} under {@code column} in {@code row}.
     * 
     * @param row
     * @param column
     * @param value
     * @return the previous value located at the intersection of {@code row} and
     *         {@code column} or {@code null} if one did not previously exist.
     */
    public V put(R row, C column, V value);

}
