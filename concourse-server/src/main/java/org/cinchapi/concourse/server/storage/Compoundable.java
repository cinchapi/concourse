/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

/**
 * A Compoundable store can perform AtomicOperations.
 * 
 * @author jnelson
 */
public interface Compoundable extends
        PermanentStore,
        VersionGetter,
        VersionChangeNotifier {

    /**
     * This method returns a log of revisions in {@code record} as
     * a Map WITHOUT grabbing any locks. This method is ONLY appropriate
     * to call from the methods of {@link #AtomicOperation} class
     * because in this case intermediate read {@link #Lock} is not required.
     * 
     * @param record
     * @return {@code Map}
     */
    public Map<Long, String> auditUnsafe(long record);

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
    public Map<Long, String> auditUnsafe(String key, long record);

    /**
     * This method returns a log of revisions in {@code record} as
     * a Map WITHOUT grabbing any locks. This method is ONLY appropriate
     * to call from the methods of {@link #AtomicOperation} class
     * because in this case intermediate read {@link #Lock} is not required.
     * 
     * @param record
     * @return {@code Map}
     */
    public Map<String, Set<TObject>> browseUnsafe(long record);

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
    public Map<TObject, Set<Long>> browseUnsafe(String key);

    /**
     * Do the work to explore {@code key} {@code operator} {@code values}
     * without worry about normalizing the {@code operator} or {@code values}.
     * This method is ONLY appropriate to call from the methods of
     * {@link #AtomicOperation} class because in this case intermediate read
     * {@link #Lock} is not required.
     * 
     * @param key
     * @param operator
     * @param values
     * @return {@code Map}
     */
    public Map<Long, Set<TObject>> doExploreUnsafe(String key,
            Operator operator, TObject... values);

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
    public Set<TObject> fetchUnsafe(String key, long record);

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
    public boolean verifyUnsafe(String key, TObject value, long record);

    /**
     * Return an {@link AtomicOperation} that can be used to group actions that
     * should all succeed or fail together. Use {@link AtomicOperation#commit()}
     * to apply the action to this store or use {@link AtomicOperation#abort()}
     * to cancel.
     * 
     * @return the AtomicOperation handler
     */
    public AtomicOperation startAtomicOperation();

}
