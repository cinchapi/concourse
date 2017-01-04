/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import com.cinchapi.concourse.server.plugin.Plugin.BackgroundInformation;
import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;
import com.google.common.annotations.VisibleForTesting;

/**
 * Factory and utility methods for creating executors that are specifically
 * designed for {@link Plugin} implementations.
 * 
 * @author Jeff Nelson
 */
public final class PluginExecutors {

    /**
     * Return a new {@link BackgroundExecutor} that uses a cached thread pool.
     * 
     * @param info retrieved from
     *            {@link com.cinchapi.concourse.server.plugin.Plugin#backgroundInformation()}
     * 
     * @return the {@link BackgroundExecutor}
     */
    public static BackgroundExecutor newCachedBackgroundExecutor(Plugin plugin) {
        BackgroundInformation info = plugin.backgroundInformation();
        return newCachedBackgroundExecutor(info.outgoing(), info.responses());
    }

    /**
     * Return a new {@link BackgroundExecutor} that uses a cached thread pool.
     * 
     * @param outgoing the {@link SharedMemory} channel on which outgoing
     *            requests to the upstream service are placed
     * @param responses a queue where responses from the upstream service may be
     *            placed
     * @return the {@link BackgroundExecutor}
     */
    @VisibleForTesting
    protected static BackgroundExecutor newCachedBackgroundExecutor(
            SharedMemory outgoing,
            ConcurrentMap<AccessToken, RemoteMethodResponse> responses) {
        return new CachedBackgroundExecutor("", outgoing, responses);
    }

    private PluginExecutors() {/* no-op */}

    /**
     * Default and simple implementation of the {@link BackgroundExecutor}
     * interface that uses a cached thread pool.
     * 
     * @author Jeff Nelson
     */
    private static class CachedBackgroundExecutor implements BackgroundExecutor {

        /**
         * The {@link ExecutorService} to which requests are delegated.
         */
        private final ExecutorService delegate;

        /**
         * Construct a new instance.
         * 
         * @param environment
         * @param outgoing
         * @param responses
         */
        public CachedBackgroundExecutor(String environment,
                SharedMemory outgoing,
                ConcurrentMap<AccessToken, RemoteMethodResponse> responses) {
            ThreadFactory factory = new ThreadFactory() {

                @Override
                public Thread newThread(Runnable runnable) {
                    BackgroundThread thread = new BackgroundThread(runnable,
                            environment, outgoing, responses);
                    return thread;
                }
            };
            delegate = Executors.newCachedThreadPool(factory);
        }

        @Override
        public void execute(String environment, Runnable runnable) {
            delegate.execute(() -> {
                BackgroundThread thread = (BackgroundThread) Thread
                        .currentThread();
                thread.environment(environment);
                runnable.run();
            });
        }
    }

}
