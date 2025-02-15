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
package com.cinchapi.concourse.server.storage.db.compaction;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

/**
 * A strategy to compact {@link Segment Segments} from a
 * {@link SegmentStorageSystem}.
 * <p>
 * A {@link Compactor} attempts to optimize the storage of
 * <strong>adjacent</strong> {@link Segment Segments}. The {@link Compactor}
 * attempts to execute a {@link #compact(Segment...) strategy} against different
 * groups of {@link Segments} of varying sizes. A single group of {@link Segment
 * Segments} that can potentially be compacted are known as a
 * <strong>shift</strong>. An entire <strong>cycle</strong> is when the
 * {@link Compactor} has tried every possible shift. (e.g. every adjacent group
 * from size 1 to size {@code n} where {@code n} is the number of {@link Segment
 * Segments}.
 * </p>
 * <p>
 * The {@link Compactor} can run in two ways:
 * <ul>
 * <li>{@link #executeFullCompaction() Full} - Block until the
 * {@link SegmentStorageSystem#lock()} can be acquired while attempting an
 * entire compaction cycle</li>
 * <li>{@link #tryIncrementalCompaction() Incremental} - Run a single shift
 * within a compaction cycle if and only if the
 * {@link SegmentStorageSystem#lock()} can be acquired immediately</li>
 * </ul>
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class Compactor {

    /**
     * A {@link Queue} that contains the {@link Segment Segments} that have been
     * compacted and removed from {@link #segments}.
     * <p>
     * Returned from {@link #garbage()}.
     * </p>
     */
    private final Queue<Segment> garbage;

    /**
     * Tracks the current work shift for the {@link Compactor}.
     */
    private final Shift shift;

    /**
     * The underlying {@link SegmentStorageSystem storage} for {@link Segment
     * Segments} that may be compacted.
     */
    private final SegmentStorageSystem storage;

    /**
     * Construct a new instance.
     * 
     * @param segments
     * @param garbage
     * @param lock
     */
    protected Compactor(SegmentStorageSystem storage) {
        this.storage = storage;
        this.garbage = new LinkedList<>();
        this.shift = new Shift(0, 1);
    }

    /**
     * Attempt to run the next compaction shift.
     */
    public final void tryIncrementalCompaction() {
        if(storage.segments().size() > 2 && storage.lock().tryLock()) {
            try {
                runShift();
            }
            finally {
                storage.lock().unlock();
            }
        }
    }

    /**
     * Run the remaining shifts to complete an entire compaction cycle.
     */
    public void executeFullCompaction() {
        if(storage.segments().size() > 2) {
            do {
                storage.lock().lock();
                try {
                    runShift();
                }
                finally {
                    storage.lock().unlock();
                }
            }
            while (!(shift.index != 0 && shift.count != 1));
        }
    }

    /**
     * Return a {@link Queue} that tracks the {@link Segment Segments} that
     * have been compacted and removed from the live collection.
     * 
     * @return the compacted {@link Segment Segments}
     */
    public final Queue<Segment> garbage() {
        return garbage;
    }

    /**
     * If possible, return a {@link List} of one or more new {@link Segment
     * Segments} that, together, contain all the data from {@code segments} in a
     * more "compact" form. If the <strong>entire group of {@code segments}
     * cannot be compacted together</strong>, return {@code null}
     * (<strong>NOT</strong> an empty list).
     * <p>
     * Compaction is atomic over {@code segments}, so if a value is
     * returned, all of the {@code segments} will be marked for
     * {@link #garbage() garbage} collection and replaced within the live list
     * of {@link #segments} with those returned. Therefore, this method should
     * only attempt compaction if its appropriate to do so given all the input
     * {@code segments}.
     * </p>
     * <p>
     * For example, if only a subset of {@code segments} are eligible for
     * compaction, this method shouldn't perform any work.
     * </p>
     * 
     * @param segments
     * @return the result of the compaction or {@code null} if compaction is not
     *         possible or would produce no changes
     */
    @Nullable
    protected abstract List<Segment> compact(Segment... segments);

    /**
     * Return {@link Shift#count}.
     * 
     * @return the shift count
     */
    @VisibleForTesting
    protected final int getShiftCount() {
        return shift.count;
    }

    /**
     * Return {@link Shift#index}.
     * 
     * @return the shift index
     */
    @VisibleForTesting
    protected final int getShiftIndex() {
        return shift.index;
    }

    /**
     * If possible, attempt to {@link #compact(Segment...) compact} the
     * {@link SegmentStorageSystem#segments() segments} that are marked for
     * inclusion based on {@code index} and {@code count}.
     * 
     * @param index
     * @param count
     */
    @VisibleForTesting
    protected final void runShift(int index, int count) {
        String id = Long.toString(Time.now());
        List<Segment> segments = storage.segments();
        int limit = segments.size();
        if(segments.get(limit - 1).isMutable()) {
            // By convention, the last Segment could be #seg0, which isn't
            // eligible for compaction.
            --limit;
        }
        if(count > limit) {
            // If attempting to compact more Segments than the limit allows,
            // reset by shifting the index back to 0 and the run length to 1.
            index = 0;
            count = 1;
        }
        else if(index > limit || index + count > limit) {
            // Circle back around to the beginning of the list, but increase the
            // run length.
            index = 0;
            ++count;
        }
        else {
            Segment[] group = segments.stream().skip(index).limit(count)
                    .toArray(Segment[]::new);
            Logger.debug(
                    "**Job: {}** Attemping to perform a compaction run with the following segments: {}",
                    id, Arrays.toString(group));
            List<Segment> compacted = compact(group);
            if(compacted != null) {
                for (int i = 0; i < count; ++i) {
                    Segment removed = segments.remove(index);
                    garbage.add(removed);
                    Logger.info(
                            "**Job: {}** The compactor removed the following segment: {}",
                            id, removed);
                }
                for (int i = compacted.size() - 1; i >= 0; --i) {
                    Segment segment = compacted.get(i);
                    segments.add(index, segment);
                    storage.save(segment);
                    Logger.info(
                            "**Job: {}** The compactor added the following segment: {}",
                            id, segment);
                }
                index += (count - 1);
            }
            else {
                Logger.debug(
                        "**Job: {}** Could not perform compaction with the following segments: {}",
                        id, Arrays.toString(group));
                ++index;
            }
        }
        shift.index = index;
        shift.count = count;
    }

    /**
     * Access the {@link SegmentStorageSystem} associated with this
     * {@link Compactor}.
     * 
     * @return the {@link SegmentStorageSystem}
     */
    protected final SegmentStorageSystem storage() {
        return storage;
    }

    /**
     * If possible, attempt to {@link #compact(Segment...) compact} the
     * {@link SegmentStorageSystem#segments() segments} that are marked for
     * inclusion of the current {@link #shift}.
     */
    private void runShift() {
        runShift(shift.index, shift.count);
    }

    /**
     * Defines the work shift via the group of {@link Segment Segments} that
     * should be included in the next {@link #runShift() run}.
     *
     * @author Jeff Nelson
     */
    protected final class Shift {

        /**
         * The new {@link Segment} count for the next
         * {@link Compactor#run(int, int) run}.
         */
        private int count;

        /**
         * The new index.
         */
        private int index;

        /**
         * Construct a new instance.
         * 
         * @param index
         * @param count
         */
        public Shift(int index, int count) {
            this.index = index;
            this.count = count;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("index", index)
                    .add("count", count).toString();
        }
    }

}
