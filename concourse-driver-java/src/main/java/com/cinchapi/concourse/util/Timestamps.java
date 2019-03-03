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
package com.cinchapi.concourse.util;

import java.util.Set;

import com.cinchapi.concourse.Timestamp;
import com.google.common.collect.Iterables;

/**
 * {@link Timestamp} related utility methods.
 * 
 * @author knd
 */
public class Timestamps {

    /**
     * Search the chronological set of {@code timestamps} to return the index of
     * a contained timestamp that occurs after the {@code sought} timestamp
     * and more closely than any others.
     * <p>
     * <ul>
     * <li>If the search set is empty, this function will return {@code 0}</li>
     * <li>If the sought timestamp is smaller than every timestamp in the search
     * set, this function will return {@code 0}</li>
     * <li>If the sought timestamp is greater than every timestamp in the search
     * set, this function will return the size of the search set, which is 1
     * greater than the last index in the search set</li>
     * </ul>
     * </p>
     * 
     * @param timestamps
     * @param sought
     * @return an index of nearest successor timestamp
     */
    public static int findNearestSuccessorForTimestamp(
            Set<Timestamp> timestamps, Timestamp sought) {
        // TODO call into second method with getMicros()
        int start = 0;
        int end = timestamps.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            Timestamp stored = Iterables.get(timestamps, mid);
            if(sought.getMicros() == stored.getMicros()) {
                return mid;
            }
            else if(sought.getMicros() > stored.getMicros()) {
                start = mid + 1;
            }
            else {
                end = mid - 1;
            }
        }
        return start;
    }

    /**
     * Search the chronological set of {@code timestamps} to return the index of
     * a contained timestamp that occurs on or after the {@code sought}
     * timestamp and more closely than any others.
     * <p>
     * <ul>
     * <li>If the search set is empty, this function will return {@code 0}</li>
     * <li>If the sought timestamp is smaller than every timestamp in the search
     * set, this function will return {@code 0}</li>
     * <li>If the sought timestamp is greater than every timestamp in the search
     * set, this function will return the size of the search set, which is 1
     * greater than the last index in the search set</li>
     * </ul>
     * </p>
     * 
     * @param timestamps
     * @param sought
     * @return an index of nearest successor timestamp
     */
    public static int findNearestSuccessorForTimestamp(Set<Long> timestamps,
            long sought) {
        int start = 0;
        int end = timestamps.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            long stored = Iterables.get(timestamps, mid);
            if(sought == stored) {
                return mid;
            }
            else if(sought > stored) {
                start = mid + 1;
            }
            else {
                end = mid - 1;
            }
        }
        return start;
    }

}
