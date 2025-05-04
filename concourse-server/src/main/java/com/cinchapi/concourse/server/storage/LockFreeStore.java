/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;

/**
 * A {@link Store} that provides versions of each present state read method that
 * can execute <strong>without</strong> grabbing locks.
 * <p>
 * NOTE: Historical reads never require logical locking.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface LockFreeStore extends Store {

    /**
     * Browse {@code key}.
     * This method returns a mapping from each of the values that is
     * currently indexed to {@code key} to a Set the records that contain
     * {@code key} as the associated value. If there are no such values, an
     * empty Map is returned. This method is ONLY appropriate to call from
     * the methods of {@link #AtomicOperation} class because in this case
     * intermediate read {@link #Lock} is not required.
     * 
     * @param key
     * @return {@code Map}
     */
    public Map<TObject, Set<Long>> browseUnlocked(String key);

    /**
     * Return a time series that contains the values stored for {@code key} in
     * {@code record} at each modification timestamp between {@code start}
     * (inclusive) and {@code end} WITHOUT grabbing any locks.
     * 
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate read
     * {@link #Lock} is not required.
     * 
     * @param key the field name
     * @param record the record id
     * @param start the start timestamp (inclusive)
     * @param end the end timestamp (exclusive)
     * @return a {@link Map mapping} from modification timestamp to a non-empty
     *         {@link Set} of values that were contained at that timestamp
     */
    public Map<Long, Set<TObject>> chronologizeUnlocked(String key, long record,
            long start, long end);

    /**
     * Do the work to explore {@code key} {@code operator} {@code values}
     * without worry about normalizing the {@code operator} or {@code values}.
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate read
     * {@link #Lock} is not required.
     * 
     * @param key
     * @param aliases
     * @return {@code Map}
     */
    public Map<Long, Set<TObject>> exploreUnlocked(String key, Aliases aliases);

    /**
     * Gather {@code key} from {@code record}.
     * <p>
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link describe(long)}. This method
     * is ONLY appropriate to call from the methods of {@link #AtomicOperation}
     * class because in this case intermediate read {@link #Lock} is not
     * required.
     * </p>
     * 
     * @param key
     * @param record
     * @return {@code Set}
     */
    public Set<TObject> gatherUnlocked(String key, long record);

    /**
     * This method returns a log of revisions in {@code record} as
     * a Map WITHOUT grabbing any locks. This method is ONLY appropriate
     * to call from the methods of {@link #AtomicOperation} class
     * because in this case intermediate read {@link #Lock} is not required.
     * 
     * @param record
     * @return {@code Map}
     */
    public Map<Long, List<String>> reviewUnlocked(long record);

    /**
     * Audit {@code key} in {@code record}. This method returns a log of
     * revisions in {@code record} as a Map WITHOUT grabbing any locks.
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate
     * read {@link #Lock} is not required.
     * 
     * @param key
     * @param record
     * @return {@code Map}
     */
    public Map<Long, List<String>> reviewUnlocked(String key, long record);

    /**
     * This method returns a log of revisions in {@code record} as
     * a Map WITHOUT grabbing any locks. This method is ONLY appropriate
     * to call from the methods of {@link #AtomicOperation} class
     * because in this case intermediate read {@link #Lock} is not required.
     * 
     * @param record
     * @return {@code Map}
     */
    public Map<String, Set<TObject>> selectUnlocked(long record);

    /**
     * Fetch {@code key} from {@code record}.
     * This method returns the values currently mapped from {@code key} in
     * {@code record}. The returned Set is nonempty if and only if {@code key}
     * is a member of the Set returned from {@link describe(long)}.
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case
     * intermediate read {@link #Lock} is not required.
     * 
     * @param key
     * @param record
     * @return {@code Set}
     */
    public Set<TObject> selectUnlocked(String key, long record);

    /**
     * Verify {@code key} equals {@code value} in {@code record}.
     * This method checks that there is currently a mapping from {@code key} to
     * {@code value} in {@code record}. This method has the same affect as
     * calling {@link fetch(String, long)} {@link Set.contains(Object)}.
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate read
     * {@link #Lock} is not required.
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code boolean}
     */
    public default boolean verifyUnlocked(String key, TObject value,
            long record) {
        return verifyUnlocked(Write.notStorable(key, value, record));
    }

    public boolean verifyUnlocked(Write write);

}
