/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.data.sort;

/**
 * A {@link Sortable} object can be {@link Sorter#sort(java.util.Map) sorted} by
 * a {@link Sorter}.
 * <p>
 * A {@link Sorter} is used to sort a Map&lt;Long, Map&lt;String, T&gt;&gt;, so
 * other objects that implement this interface must define how the
 * {@link Sorter} construct applies to its structure.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Sortable<T> {

    /**
     * Sort this object using the {@code sorter}
     * 
     * @param sorter
     */
    public void sort(Sorter<T> sorter);

    /**
     * Sort this object using the {@code sorter} with the {@code at} timestamp
     * as a contextual reference.
     * 
     * @param sorter
     * @param at
     */
    public void sort(Sorter<T> sorter, long at);

}
