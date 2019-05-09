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
package com.cinchapi.concourse.server.query;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class provides utility methods for sorting a dataset
 */
public class Sorter {

    /**
     * Sorts {@code data} based on {@code order}
     *
     * @param data the data to sort
     * @param order the sort order
     * @return the sorted data
     */
    public final static Map<Long, Map<String, Set<TObject>>>  sort(
            Map<Long, Map<String, Set<TObject>>> data, Order order) {

        List<Comparator<Map<String, Set<TObject>>>> comparators = Lists.newArrayList();
        for (Map.Entry<String, Integer> keyOrder : order.getSpec().entrySet()) {
            Comparator<Map<String, Set<TObject>>> comparator = new Comparator<Map<String, Set<TObject>>>() {
                @Override
                public int compare(Map<String, Set<TObject>> o1,
                        Map<String, Set<TObject>> o2) {
                    if(!o1.containsKey(keyOrder.getKey())) {
                        return 1;
                    }
                    else if(!o2.containsKey(keyOrder.getKey())) {
                        return -1;
                    }

                    Iterator<TObject> iterator = o1.get(keyOrder.getKey()).iterator();
                    TObject object = iterator.next();
                    switch (object.getType()) {
                    case BOOLEAN:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Boolean.class);
                    case DOUBLE:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Double.class);
                    case FLOAT:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Float.class);
                    case INTEGER:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Integer.class);
                    case LINK:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Link.class);
                    case LONG:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Long.class);
                    case TAG:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Tag.class);
                    case TIMESTAMP:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                Timestamp.class);
                    case NULL:
                        return 0;
                    default:
                        return compareTo(o1, o2, keyOrder.getValue(),
                                String.class);
                    }
                }

                /**
                 * Performs a comparison of two record entries using java objects
                 * based on a key. The records are first compared on the size
                 * of their respective value sets with value sets with greater
                 * quantity taking priority over less quantities. Then the
                 * quantities are compared on an in-order basis.
                 *
                 * @param o1
                 * @param o2
                 * @param direction
                 * @param type
                 * @param <T>
                 * @return
                 */
                private <T extends Comparable<? super T>> int compareTo(
                        Map<String, Set<TObject>> o1,
                        Map<String, Set<TObject>> o2, Integer direction,
                        Class<T> type) {
                    Set<T> values1 = Transformers
                            .transformSetLazily(o1.get(keyOrder.getKey()),
                                    Conversions.thriftToJavaCasted());
                    Set<T> values2 = Transformers
                            .transformSetLazily(o2.get(keyOrder.getKey()),
                                    Conversions.thriftToJavaCasted());

                    if (Direction.ASCENDING.coefficient() == direction) {
                        Iterator<T> it1 = values1.iterator();
                        Iterator<T> it2 = values2.iterator();
                        while (it1.hasNext() && it2.hasNext()) {
                            T v1 = it1.next();
                            T v2 = it2.next();

                            if(v1.compareTo(v2) != 0) {
                                return v1.compareTo(v2);
                            }
                        }

                        if (it1.hasNext()) {
                            return -1;
                        }
                        else if (it2.hasNext()) {
                            return 1;
                        }
                        else {
                            return 0;
                        }
                    }
                    else if (Direction.DESCENDING.coefficient() == direction) {
                        Iterator<T> it1 = values1.iterator();
                        Iterator<T> it2 = values2.iterator();
                        while (it1.hasNext() && it2.hasNext()) {
                            T v1 = it1.next();
                            T v2 = it2.next();

                            if(v2.compareTo(v1) != 0) {
                                return v2.compareTo(v1);
                            }
                        }

                        if (it1.hasNext()) {
                            return 1;
                        }
                        else if (it2.hasNext()) {
                            return -1;
                        }
                        else {
                            return 0;
                        }
                    }
                    else {
                        return 0;
                    }
                }
            };

            comparators.add(comparator);
        }



        data = data.entrySet().stream().sorted(Map.Entry
                .comparingByValue(new MultiComparator<>(comparators))).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e2, LinkedHashMap::new));

        return data;
    }

    /**
     * This class provides the ability to compare on multiple fields
     *
     * @param <T>
     */
    public static class MultiComparator<T> implements Comparator<T> {
        private List<Comparator<T>> comparators;

        public MultiComparator(List<Comparator<T>> comparators) {
            this.comparators = comparators;
        }

        public int compare(T o1, T o2) {
            for (Comparator<T> comparator : comparators) {
                int comparison = comparator.compare(o1, o2);
                if (comparison != 0) return comparison;
            }
            return 0;
        }
    }

}
