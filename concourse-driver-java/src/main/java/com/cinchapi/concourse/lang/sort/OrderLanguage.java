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
package com.cinchapi.concourse.lang.sort;

import java.util.List;
import java.util.Map;

import com.cinchapi.concourse.thrift.TOrder;
import com.google.common.collect.Lists;

/**
 * Tools for translating aspects of the language.
 *
 * @author Jeff Nelson
 */
public class OrderLanguage {

    /**
     * Translate the {@code order} to its Thrift analog.
     *
     * @param order
     * @return the analogous TOrder
     */
    public static TOrder translateToThriftOrder(Order order) {
        Map<String, Integer> spec = order.getSpec();
        List<String> keys = Lists.newArrayList();
        List<Integer> directions = Lists.newArrayList();
        for (Map.Entry<String, Integer> entry : spec.entrySet()) {
            keys.add(entry.getKey());
            directions.add(entry.getValue());
        }
        return new TOrder(keys, directions);
    }

    /**
     * Translate the {@code torder} to its Java analog.
     *
     * @param torder
     * @return the analogous Java {@link Order}
     */
    public static Order translateFromThriftOrder(TOrder torder) {
        List<String> keys = torder.getKeys();
        List<Integer> directions = torder.getDirections();
        Order order = new Order();
        for (int i = 0; i < keys.size(); i++) {
            order.add(keys.get(i), directions.get(i) == -1
                    ? Direction.DESCENDING : Direction.ASCENDING);
        }

        return order;
    }
}
