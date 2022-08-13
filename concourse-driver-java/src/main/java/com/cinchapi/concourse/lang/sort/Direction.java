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
package com.cinchapi.concourse.lang.sort;

/**
 * Sort directions.
 *
 * @author Jeff Nelson
 */
public enum Direction {

    ASCENDING(1), DESCENDING(-1);

    /**
     * Return the default {@link Direction}.
     * 
     * @return the default
     */
    static Direction $default() {
        return Direction.ASCENDING;
    }

    /**
     * The coefficient is multiplied by the result of a {@link Comparator} to
     * sort elements in forward or reverse order.
     */
    private final int coefficient;

    /**
     * Construct a new instance.
     * 
     * @param coefficient
     */
    Direction(int coefficient) {
        this.coefficient = coefficient;
    }

    /**
     * Return the coefficient associated with this {@link Direction}.
     * 
     * @return the coefficient
     */
    public int coefficient() {
        return coefficient;
    }

}
