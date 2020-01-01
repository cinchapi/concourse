/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.util.Map;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Sorter} imposes an {@link Order} by value on a result set.
 *
 * @author Jeff Nelson
 */
public interface Sorter<V> {

    /**
     * Sort the data.
     * 
     * @param data
     * @return the sorted data
     */
    public Map<Long, Map<String, V>> sort(Map<Long, Map<String, V>> data);

    /**
     * Sort the data using the {@code at} timestamp as temporal binding for
     * missing value lookups when an order component does not explicitly specify
     * a timestamp.
     * 
     * @param data
     * @param at
     * @return the sorted data
     */
    public Map<Long, Map<String, V>> sort(Map<Long, Map<String, V>> data,
            @Nullable Long at);

}
