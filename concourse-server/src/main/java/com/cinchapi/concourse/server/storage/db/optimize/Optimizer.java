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
package com.cinchapi.concourse.server.storage.db.optimize;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.storage.db.disk.Segment;
import com.cinchapi.concourse.util.Logger;

/**
 * An {@link Optimizer} provides a strategy for optimizing {@link Segment
 * Segments}.
 *
 * @author Jeff Nelson
 */
public abstract class Optimizer {

    /**
     * The index of the "a" {@link Segment} being considered for optimization.
     */
    private int aindex;

    /**
     * The index of the "b" {@link Segment} being considered for optimization.
     */
    private int bindex;

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
     * {@link #segments}. The {@link Optimizer} itself does not know anything
     * about the concurrency characteristics of the {@link #segments}
     * collection, so it is assumed that the {@link Lock} will guard against
     * external entities modifying the collection when the optimizer is doing
     * so.
     */
    private Lock lock;

    /**
     * Flag that indicates that the {@link Optimizer} is running.
     */
    private AtomicBoolean running;

    /**
     * Provided list of {@link Segment Segments} that will attempt to be
     * optimized.
     */
    private final List<Segment> segments;

    /**
     * An object used to alert the {@link #worker} that there may be work to do.
     */
    private final Object signal;

    /**
     * A dedicated {@link Thread} that performs optimization tasks.
     */
    private Thread worker;

    /**
     * Construct a new instance.
     * 
     * @param segments
     * @param garbage
     * @param lock
     */
    protected Optimizer(String environment, List<Segment> segments,
            List<Segment> garbage, Lock lock, Supplier<Path> fileProvider) {
        this.environment = environment;
        this.segments = segments;
        this.garbage = garbage;
        this.lock = lock;
        this.signal = new Object();
        this.fileProvider = fileProvider;
    }

    /**
     * If possible merge {@link Segment Segments} {@code a} and {@code b} and
     * return a new {@link Segment} with a union of their data. If merging the
     * two {@link Segment Segments} would not provide any optimizations, return
     * {@code null}.
     * 
     * @param a
     * @param b
     * @return a new {@link Segment} that is the merger of {@code a} and
     *         {@code b} if appropriate; otherwise {@code null}
     */
    @Nullable
    public abstract Segment merge(Segment a, Segment b);

    /**
     * Start the {@link Optimizer}
     */
    public void start() {
        running.set(true);
        bindex = 1;
        aindex = bindex - 1;
        worker = new Thread(() -> {
            while (running.get()) {
                if(Thread.interrupted()) {
                    break;
                }
                if(segments.size() >= 3) {
                    Path file = fileProvider.get();
                    File _file = file.toFile();
                    Segment a = segments.get(aindex);
                    Segment b = segments.get(bindex);
                    StorageContext context = new StorageContext() {

                        @Override
                        public long availableDiskSpace() {
                            return _file.getUsableSpace();
                        }

                        @Override
                        public List<Segment> segments() {
                            return Collections.unmodifiableList(
                                    segments.stream().filter(Segment::isMutable)
                                            .collect(Collectors.toList()));
                        }

                        @Override
                        public long totalDiskSpace() {
                            return _file.getTotalSpace();
                        }

                    };
                    if(isTriggered(context)) {
                        // replace Segment with Segments
                        // replace Segments[] with Segment
                        Segment merged = isOptimizationPossible(context, a, b)
                                ? merge(a, b) : null;
                        if(merged != null) {
                            lock.lock();
                            try {
                                merged.fsync(fileProvider.get());
                                segments.set(aindex, merged);
                                segments.remove(bindex);
                                bindex = segments.size() - 1 > bindex ? bindex
                                        : 1;
                                aindex = bindex - 1;
                                garbage.add(a);
                                garbage.add(b);
                                Logger.info("Merged Segments {} and {} into {}",
                                        a, b, merged);
                            }
                            finally {
                                lock.unlock();
                            }
                        }
                        else {
                            bindex = segments.size() - 1 > bindex ? bindex + 1
                                    : 1;
                            aindex = bindex - 1;
                        }
                    }
                    Thread.yield();
                }
                else {
                    try {
                        signal.wait();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }, AnyStrings.format("Storage Optimizer [{}]", environment));
        worker.setDaemon(true);
        worker.setPriority(Thread.MIN_PRIORITY);
        worker.setUncaughtExceptionHandler((thread, exception) -> {
            Logger.error("Uncaught exception in {}:", thread.getName(),
                    exception);
            Logger.error(
                    "{} has STOPPED WORKING due to an unexpected exception. Storage Optmization is paused untilt the error is resolved",
                    thread.getName());
        });
        worker.start();
    }

    /**
     * Stop the {@link Optimizer}.
     */
    public void stop() {
        if(running.compareAndSet(true, false)) {
            worker.interrupt();
        }
    }

    /**
     * Return {@code true} if it is possible to run optimization on all the
     * {@code segments} given the {@link Database} context.
     * 
     * @param context
     * @param segments
     * @return a boolean that indicates if it is possible for the optimizer to
     *         optimize the {@code segments}
     */
    protected abstract boolean isOptimizationPossible(StorageContext context,
            Segment... segments);

    /**
     * Return {@code true} if this {@link Optimizer} should run, given the
     * {@link Database} context.
     * 
     * @param context
     * @return a boolean that indicates if the optimizer should run
     */
    protected abstract boolean isTriggered(StorageContext context);

}
