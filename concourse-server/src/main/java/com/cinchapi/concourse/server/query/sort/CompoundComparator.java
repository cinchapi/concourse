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
package com.cinchapi.concourse.server.query.sort;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link Comparator} that tests using an ordered list of other comparators;
 * returning the first non-zero value it can find. If all the comparators deem
 * two objects to be equal, {@code 0} is returned.
 *
 * @author Jeff Nelson
 */
public class CompoundComparator<T> implements Comparator<T> {

    /**
     * Return a {@link CompundComparator} composed of the provided
     * {@code comparators}.
     * 
     * @param comparators
     * @return the {@link CompoundComparator}
     */
    @SafeVarargs
    public static <T> CompoundComparator<T> of(Comparator<T>... comparators) {
        return new CompoundComparator<>(Arrays.asList(comparators));
    }

    /**
     * The ordered list of comparators to apply.
     */
    private final List<Comparator<T>> comparators;

    /**
     * Construct a new instance.
     * 
     * @param comparators
     */
    private CompoundComparator(List<Comparator<T>> comparators) {
        this.comparators = comparators;
    }

    @Override
    public int compare(T o1, T o2) {
        int c = 0;
        for (Comparator<T> comparator : comparators) {
            c = comparator.compare(o1, o2);
            if(c != 0) {
                break;
            }
        }
        return c;
    }

}
