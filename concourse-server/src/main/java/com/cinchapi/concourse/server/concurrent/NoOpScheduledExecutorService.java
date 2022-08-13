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
package com.cinchapi.concourse.server.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * A {@link ScheduledExecutorService} that doesn't actually scheduled anything.
 *
 * @author Jeff Nelson
 */
// Adapted from
// https://github.com/google/guava/blob/master/guava-testlib/src/com/google/common/util/concurrent/testing/TestingExecutors.java
public final class NoOpScheduledExecutorService extends AbstractExecutorService
        implements
        ScheduledExecutorService {

    /**
     * Return a {@link NoOpScheduledExecutorService}.
     * 
     * @return {@link NoOpScheduledExecutorService}
     */
    public static NoOpScheduledExecutorService instance() {
        return INSTANCE;
    }

    /**
     * Singleton.
     */
    private static NoOpScheduledExecutorService INSTANCE = new NoOpScheduledExecutorService();

    private volatile boolean shutdown = false;

    /**
     * Construct a new instance.
     */
    private NoOpScheduledExecutorService() {/* no-init */}

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return true;
    }

    @Override
    public void execute(Runnable command) {/* no-op */}

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
            TimeUnit unit) {
        return new NeverScheduledFuture<>();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay,
            TimeUnit unit) {
        return new NeverScheduledFuture<>();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
            long initialDelay, long period, TimeUnit unit) {
        return new NeverScheduledFuture<>();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
            long initialDelay, long delay, TimeUnit unit) {
        return new NeverScheduledFuture<>();
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return ImmutableList.of();
    }

    /**
     * Represented a future task that is never actually scheduled.
     *
     * @author Jeff Nelson
     */
    private static class NeverScheduledFuture<V> extends AbstractFuture<V>
            implements
            ScheduledFuture<V> {

        @Override
        public int compareTo(Delayed other) {
            return Longs.compare(getDelay(TimeUnit.NANOSECONDS),
                    other.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

    }

}
