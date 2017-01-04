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

import java.util.concurrent.Executor;

/**
 * A {@link BackgroundExecutor} executes submitted {@link Runnable tasks} using
 * a
 * specific environment when making requests to the upstream
 * {@link ConcourseRuntime service}.
 * 
 * @author Jeff Nelson
 */
public interface BackgroundExecutor extends Executor {

    /**
     * <strong>DO NOT CALL!</strong>
     * <p>
     * Use {@link #execute(String, Runnable)} instead.
     * </p>
     */
    @Override
    public default void execute(Runnable runnable) {
        throw new UnsupportedOperationException();
    }

    /**
     * Execute the given {@code command} at some point in the future, using the
     * provided {@code environment} when making upstream requests.
     * 
     * @param environment
     * @param command
     */
    public void execute(String environment, Runnable command);

}
