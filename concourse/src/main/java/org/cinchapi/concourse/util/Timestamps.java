/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import java.util.Set;

import org.cinchapi.concourse.Timestamp;

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
        int start = 0;
        int end = timestamps.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            Timestamp stored = Iterables.get(timestamps, mid);
            if(stored.getMicros() == sought.getMicros()) {
                return mid + 1;
            }
            else if(stored.getMicros() < sought.getMicros()) {
                start = mid + 1;
            }
            else {
                end = mid - 1;
            }
        }
        return start;
    }

}
