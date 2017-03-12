/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Yet another utility class that deals with {@link Collection Collections}.
 * 
 * @author Jeff Nelson
 */
public final class Collections {

    /**
     * Convert the given {@code collection} to a {@link List}, if necessary.
     * 
     * @param collection
     * @return a List that contains all the elements in {@code collection}.
     */
    public static <T> List<T> toList(Collection<T> collection) {
        return collection instanceof List ? (List<T>) collection : Lists
                .newArrayList(collection);
    }

    /**
     * Convert the given {@code collection} to a {@link List} of {@link Long}
     * values, if necessary. The client must attempt to do this for every
     * collection of records because CaSH always passes user input in as a
     * collection of {@link Integer integers}.
     * 
     * @param collection
     * @return a List that contains Long values
     */
    public static <T> List<Long> toLongList(Collection<T> collection) {
        List<Long> list = Lists.newArrayListWithCapacity(collection.size());
        Iterator<T> it = collection.iterator();
        while (it.hasNext()) {
            T elt = it.next();
            if(elt instanceof Number) {
                list.add(((Number) elt).longValue());
            }
            else {
                throw new ClassCastException("Cant convert object of type"
                        + elt.getClass() + " to java.lang.Long");
            }
        }
        return list;
    }

    private Collections() {/* noop */}

}
