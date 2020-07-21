/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.util.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * An {@link Compactor} provides a strategy for optimizing {@link Segment
 * Segments}.
 *
 * @author Jeff Nelson
 */
public abstract class Compactor {

    /**
     * Return a {@link Compactor} builder.
     * 
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link Compactor Compactors}.
     *
     * @author Jeff Nelson
     */
    public static class Builder {

        private Class<? extends Compactor> type;
        private String environment;
        private List<Segment> segments;
        private List<Segment> garbage;
        private Lock lock;
        private Supplier<Path> fileProvider;
        private long minorInitialDelayInSeconds = 5 * 60;
        private long minorRunFrequencyInSeconds = 1;
        private long majorInitialDelayInSeconds = 86400;
        private long majorRunFrequencyInSeconds = 7 * 86400;

        /**
         * Set the {@link Compactor} type to build.
         * 
         * @param type
         * @return this
         */
        public Builder type(Class<? extends Compactor> type) {
            this.type = type;
            return this;
        }

        /**
         * Set the {@link Compactor Compactor's} environment.
         * 
         * @param environment
         * @return this
         */
        public Builder environment(String environment) {
            this.environment = environment;
            return this;
        }

        /**
         * Provide the dynamic list of {@link Segment Segments} over which
         * compaction should run.
         * 
         * @param segments
         * @return this
         */
        public Builder segments(List<Segment> segments) {
            this.segments = segments;
            return this;
        }

        /**
         * Provide a {@link Lock} to use for concurrency control.
         * 
         * @param lock
         * @return this
         */
        public Builder lock(Lock lock) {
            this.lock = lock;
            return this;
        }

        /**
         * Specify the {@link Supplier} that generates file {@link Path Paths}
         * for {@link Segment Segments} that are created during compaction.
         * 
         * @param fileProvider
         * @return this
         */
        public Builder fileProvider(Supplier<Path> fileProvider) {
            this.fileProvider = fileProvider;
            return this;
        }

        /**
         * Specify how long the {@link Compactor} should wait before scheduling
         * the first minor compaction.
         * 
         * @param minorInitialDelayInSeconds
         * @return this
         */
        public Builder minorCompactionInitialDelayInSeconds(
                long minorInitialDelayInSeconds) {
            this.minorInitialDelayInSeconds = minorInitialDelayInSeconds;
            return this;
        }

        /**
         * Specify how long the {@link Compactor} should wait before scheduling
         * the first major compaction.
         * 
         * @param minorInitialDelayInSeconds
         * @return this
         */
        public Builder majorCompactionInitialDelayInSeconds(
                long majorInitialDelayInSeconds) {
            this.majorInitialDelayInSeconds = majorInitialDelayInSeconds;
            return this;
        }

        /**
         * Specify how long the {@link Compactor} should wait after an attempt
         * (successful or not) at a minor compaction before attempting again.
         * 
         * @param minorRunFrequencyInSeconds
         * @return this
         */
        public Builder minorCompactionRunFrequencyInSeconds(
                long minorRunFrequencyInSeconds) {
            this.minorRunFrequencyInSeconds = minorRunFrequencyInSeconds;
            return this;
        }

        /**
         * Specify how long the {@link Compactor} should wait after an attempt
         * (successful or not) at a major compaction before attempting again.
         * 
         * @param minorRunFrequencyInSeconds
         * @return this
         */
        public Builder majorCompactionRunFrequencyInSeconds(
                long majorRunFrequencyInSeconds) {
            this.majorRunFrequencyInSeconds = majorRunFrequencyInSeconds;
            return this;
        }

        /**
         * Provide the collection that maintains the {@link Segments} that
         * should be discarded.
         * 
         * @param garbage
         * @return this
         */
        public Builder garbage(List<Segment> garbage) {
            this.garbage = garbage;
            return this;
        }

        /**
         * Build and return the {@link Compactor}
         * 
         * @return the built {@link Compactor}
         */
        public Compactor build() {
            Preconditions.checkState(!Empty.ness().describes(environment));
            Preconditions.checkState(segments != null);
            Preconditions.checkState(garbage != null);
            Preconditions.checkState(lock != null);
            Preconditions.checkState(fileProvider != null);
            return Reflection.newInstance(type, environment, segments, garbage,
                    lock, fileProvider, minorInitialDelayInSeconds,
                    minorRunFrequencyInSeconds, majorInitialDelayInSeconds,
                    majorRunFrequencyInSeconds);
        }

    }

    /**
     * Return a {@link ThreadFactory} that produces threads for
     * {@link #major} within the specified {@code environment}.
     * 
     * @param environment
     * @return the {@link ThreadFactory}
     */
    private static ThreadFactory majorThreadFactory(String environment) {
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        tfb.setDaemon(true);
        tfb.setNameFormat(
                AnyStrings.joinSimple("MajorCompaction [", environment, "]"));
        tfb.setPriority(Thread.MAX_PRIORITY);
        tfb.setUncaughtExceptionHandler((thread, exception) -> {
            Logger.error("Uncaught exception in {}:", thread.getName(),
                    exception);
            Logger.error(
                    "{} has STOPPED WORKING due to an unexpected exception. Major compaction is paused until the error is resolved",
                    thread.getName());
        });
        return tfb.build();
    }

    /**
     * Return a {@link ThreadFactory} that produces threads for
     * {@link #minor} within the specified {@code environment}.
     * 
     * @param environment
     * @return the {@link ThreadFactory}
     */
    private static ThreadFactory minorThreadFactory(String environment) {
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
        tfb.setDaemon(true);
        tfb.setNameFormat(
                AnyStrings.joinSimple("MinorCompaction [", environment, "]"));
        tfb.setPriority(Thread.MIN_PRIORITY);
        tfb.setUncaughtExceptionHandler((thread, exception) -> {
            Logger.error("Uncaught exception in {}:", thread.getName(),
                    exception);
            Logger.error(
                    "{} has STOPPED WORKING due to an unexpected exception. Major compaction is paused until the error is resolved",
                    thread.getName());
        });
        return tfb.build();
    }

    /**
     * The name of the environment.
     */
    private final String environment;

    /**
     * Provides files for new {@link Segments} to be stored in.
     */
    private final Supplier<Path> fileProvider;

    /**
     * Provided collection for {@link Segment Segments} that are discarded.
     */
    private final List<Segment> garbage;

    /**
     * A {@link Lock} that is provided to control access to the
     * {@link #segments}. The {@link Compactor} itself does not know anything
     * about the concurrency characteristics of the {@link #segments}
     * collection, so it is assumed that the {@link Lock} will guard against
     * external entities modifying the collection when the optimizer is doing
     * so.
     */
    private Lock lock;

    /**
     * An {@link Executor} that schedules major compactions.
     */
    private ScheduledExecutorService major;

    /**
     * The number of {@link Segment Segments} in the run for the next major
     * compaction.
     */
    private int majorCount;

    /**
     * The index of the {@link Segment} that starts the run for the next major
     * compaction.
     */
    private int majorIndex;

    /**
     * The number of seconds to wait before running the first {@link #major}
     * compaction.
     */
    private final long majorInitialDelayInSeconds;

    /**
     * The number of seconds to wait between two runs of a {@link #major}
     * compactions.
     */
    private final long majorRunFrequencyInSeconds;

    /**
     * An {@link Executor} that schedules minor compactions.
     */
    private ScheduledExecutorService minor;

    /**
     * The number of {@link Segment Segments} in the run for the next minor
     * compaction.
     */
    private int minorCount;

    /**
     * The index of the {@link Segment} that starts the run for the next minor
     * compaction.
     */
    private int minorIndex;

    /**
     * The number of seconds to wait before running the first {@link #minor}
     * compaction.
     */
    private final long minorInitialDelayInSeconds;

    /**
     * The number of seconds to wait between two runs of a {@link #minor}
     * compactions.
     */
    private final long minorRunFrequencyInSeconds;

    /**
     * Provided list of {@link Segment Segments} that will attempt to be
     * optimized.
     */
    private final List<Segment> segments;

    /**
     * Construct a new instance.
     * 
     * @param segments
     * @param garbage
     * @param lock
     */
    protected Compactor(String environment, List<Segment> segments,
            List<Segment> garbage, Lock lock, Supplier<Path> fileProvider,
            long minorInitialDelayInSeconds, long minorRunFrequencyInSeconds,
            long majorInitialDelayInSeconds, long majorRunFrequencyInSeconds) {
        this.environment = environment;
        this.segments = segments;
        this.garbage = garbage;
        this.lock = lock;
        this.fileProvider = fileProvider;
        this.minorInitialDelayInSeconds = minorInitialDelayInSeconds;
        this.majorInitialDelayInSeconds = majorInitialDelayInSeconds;
        this.minorRunFrequencyInSeconds = minorRunFrequencyInSeconds;
        this.majorRunFrequencyInSeconds = majorRunFrequencyInSeconds;
    }

    /**
     * Start the {@link Compactor}.
     */
    public void start() {
        minorIndex = 0;
        minorCount = 1;
        majorIndex = 0;
        majorCount = 1;

        minor = Executors.newScheduledThreadPool(1,
                minorThreadFactory(environment));
        minor.scheduleWithFixedDelay(() -> {
            /*
             * Minor compaction is an minor job that only runs if the lock
             * can be acquired immediately and only attempts one run of
             * Segments.
             */
            if(segments.size() > 2) {
                if(lock.tryLock()) {
                    try {
                        Shift shift = run(minorIndex, minorCount);
                        minorIndex = shift.index;
                        minorCount = shift.count;
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        }, minorInitialDelayInSeconds, minorRunFrequencyInSeconds,
                TimeUnit.SECONDS);

        major = Executors.newScheduledThreadPool(1,
                majorThreadFactory(environment));
        major.scheduleWithFixedDelay(() -> {
            /*
             * Major compaction runs an entire round of compaction (e.g. it
             * tries to #compact every possible run of Segments) and blocks
             * until the lock can be acquired.
             */
            if(segments.size() > 2) {
                for (;;) {
                    lock.lock();
                    try {
                        Shift shift = run(majorIndex, majorCount);
                        if(shift.index == 0 && shift.count == 1) {
                            break;
                        }
                        else {
                            majorIndex = shift.index;
                            majorCount = shift.count;
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        }, majorInitialDelayInSeconds, majorRunFrequencyInSeconds,
                TimeUnit.SECONDS);
    }

    /**
     * Stop the {@link Compactor}.
     */
    public void stop() {
        for (ExecutorService executor : ImmutableSet.of(minor, major)) {
            executor.shutdown();
        }

    }

    /**
     * If possible, compact the group of {@code segments} using this
     * {@link Compactor Compactor's} strategy.
     * <p>
     * If the {@code segments} can be compacted, return a list of new
     * {@link Segment segments} that represent the result of the compaction.
     * </p>
     * <p>
     * If the {@code segments} cannot be compacted, return {@code null}.
     * </p>
     * <p>
     * Compaction is atomic over the group of {@code segments}. If a value is
     * returned, the {@link Segment Segments} in the list will replace all of
     * the input {@code segments}. Therefore, this method should only attempt
     * compaction if its appropriate to do so given all the input
     * {@code segments}.
     * </p>
     * <p>
     * For example, if only a subset of {@code segments} are eligible for
     * compaction, this method shouldn't perform any work. The {@link Compactor}
     * ultimately ensures that all possible {@link Segment} runs are given a
     * chance to be compacted as a group.
     * </p>
     * 
     * @param context
     * @param segments
     * @return the result of the compaction or {@code null} if compaction is not
     *         possible
     */
    @Nullable
    protected abstract List<Segment> compact(StorageContext context,
            Segment... segments);

    /**
     * Run the {@link #compact(StorageContext, Segment...) compaction} for
     * {@code count} {@link #segments} starting at {@code index}.
     * 
     * @param index
     * @param count
     * @return the {@link Shift} for the {@code index} and {@code count}
     */
    @VisibleForTesting
    protected Shift run(int index, int count) {
        String id = UUID.randomUUID().toString();
        int limit = segments.size() - 2; // Cannot compact #seg0
        if(count > limit) {
            index = 0;
            count = 1;
        }
        else if(index > limit || index + count > limit) {
            index = 0;
            ++count;
        }
        else {
            Segment[] group = segments.stream().skip(index).limit(count)
                    .toArray(Segment[]::new);
            Logger.debug(
                    "**Job: {}** Attemping to perform a compaction run with the following segments: {}",
                    id, Arrays.toString(group));
            File file = fileProvider.get().toFile();
            StorageContext context = new StorageContext() {

                @Override
                public long availableDiskSpace() {
                    return file.getUsableSpace();
                }

                @Override
                public long totalDiskSpace() {
                    return file.getTotalSpace();
                }

            };
            List<Segment> compacted = compact(context, group);
            if(compacted != null) {
                for (int i = 0; i <= count; ++i) {
                    Segment removed = segments.remove(index);
                    garbage.add(removed);
                    Logger.info(
                            "**Job: {}** The compactor removed the following segment: {}",
                            id, removed);
                }
                for (int i = compacted.size() - 1; i >= 0; --i) {
                    Segment segment = compacted.get(i);
                    segment.fsync(fileProvider.get());
                    segments.add(index, segment);
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
        return new Shift(index, count);
    }

    /**
     * Return from {@link Compactor#run(int, int)} to indicate how the tracked
     * index and group size should shift.
     *
     *
     * @author Jeff Nelson
     */
    protected final class Shift {

        /**
         * The new {@link Segment} count for the next
         * {@link Compactor#run(int, int) run}.
         */
        protected final int count;

        /**
         * The new index.
         */
        protected final int index;

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
