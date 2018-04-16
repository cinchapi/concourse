/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse.server.calculate;

import com.cinchapi.concourse.util.Numbers;

/**
 * A collection of common {@link KeyCalculation} and
 * {@link KeyRecordCalculation} functions.
 * 
 * @author Jeff Nelson
 */
public final class Calculations {

    public static void checkCalculatable(Object value) {
        if(!(value instanceof Number)) {
            // TODO throw a specific/custom exception
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Return the canonical {@link KeyCalculation calculation} for finding the
     * count of all values for a field .
     * 
     * @return the function
     */
    public static KeyCalculation countKey() {
        return (running, value, records) -> {
            return Numbers.add(running, records.size());
        };
    }

    /**
     * Return the canonical {@link KeyRecordCalculation} for finding the count
     * of all the values store for a key in a record.
     * 
     * @return the function
     */
    public static KeyRecordCalculation countKeyRecord() {
        return (running, value) -> {
            return Numbers.add(running, 1);
        };
    }

    /**
     * Return the canonical {@link KeyRecordCalculation} for finding the max
     * over all the values store for a key in a record.
     * 
     * @return the function
     */
    public static KeyRecordCalculation maxKeyRecord() {
        return (running, value) -> {
            return Numbers.max(running, value);
        };
    }

    /**
     * Return the canonical {@link KeyRecordCalculation} for finding the min
     * over all the values store for a key in a record.
     * 
     * @return the function
     */
    public static KeyRecordCalculation minKeyRecord() {
        return (running, value) -> {
            return Numbers.min(running, value);
        };
    }

    /**
     * Return the canonical {@link KeyCalculation calculation} for finding the
     * sum over an entire field.
     * 
     * @return the function
     */
    public static KeyCalculation sumKey() {
        return (running, value, records) -> {
            value = Numbers.multiply(value, records.size());
            return Numbers.add(running, value);
        };
    }

    /**
     * Return the canonical {@link KeyRecordCalculation} for finding the sum
     * over all the values store for a key in a record.
     * 
     * @return the function
     */
    public static KeyRecordCalculation sumKeyRecord() {
        return (running, value) -> {
            return Numbers.add(running, value);
        };
    }

    private Calculations() {/* no-op */}

}
